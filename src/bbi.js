"use strict";

var spans = require('./spans');
var Range = spans.Range;
var union = spans.union;

var jszlib = require('jszlib');
var jszlib_inflate_buffer = jszlib.inflateBuffer;
var arrayCopy = jszlib.arrayCopy;

var M1 = 256;
var M2 = 256*256;
var M3 = 256*256*256;
var M4 = 256*256*256*256;

var BIG_WIG_MAGIC = 0x888FFC26;
var BIG_WIG_MAGIC_BE = 0x26FC8F88;
var BIG_BED_MAGIC = 0x8789F2EB;
var BIG_BED_MAGIC_BE = 0xEBF28987;

var BIG_WIG_TYPE_GRAPH = 1;
var BIG_WIG_TYPE_VSTEP = 2;
var BIG_WIG_TYPE_FSTEP = 3;

function bwg_readOffset(ba, o) {
    return ba[o] + ba[o+1]*M1 + ba[o+2]*M2 + ba[o+3]*M3 + ba[o+4]*M4;
}

function shallowCopy(o) {
    var n = {};
    for (var k in o) {
        n[k] = o[k];
    }
    return n;
}

function connectBBI(f, opts) {
    opts = opts || {};

    var bwg = new BBISource();
    bwg.data = f;

    return f.slice(0, 512).fetch()
      .then(function(header) {
        var ba = new Uint8Array(header);
        var sa = new Int16Array(header);
        var la = new Int32Array(header);
        var magic = ba[0] + (M1 * ba[1]) + (M2 * ba[2]) + (M3 * ba[3]);
        if (magic == BIG_WIG_MAGIC) {
            bwg.type = 'bigwig';
        } else if (magic == BIG_BED_MAGIC) {
            bwg.type = 'bigbed';
        } else if (magic == BIG_WIG_MAGIC_BE || magic == BIG_BED_MAGIC_BE) {
            throw Error("Currently don't support big-endian BBI files");
        } else {
            throw Error("Not a supported format, magic=0x" + magic.toString(16));
        }

        var version = sa[2];
        if (version < 3 || version > 4) {
            throw Error("Unsupported BBI version " + version);
        }
        bwg.numZoomLevels = sa[3];       // 6
        bwg.chromTreeOffset = bwg_readOffset(ba, 8);
        bwg.unzoomedDataOffset = bwg_readOffset(ba, 16);
        bwg.unzoomedIndexOffset = bwg_readOffset(ba, 24);
        bwg.fieldCount = sa[16];         // 32
        bwg.definedFieldCount = sa[17];  // 34
        bwg.asOffset = bwg_readOffset(ba, 36);
        bwg.totalSummaryOffset = bwg_readOffset(ba, 44);
        bwg.uncompressBufSize = la[13];  // 52
        bwg.extHeaderOffset = bwg_readOffset(ba, 56);

        bwg.zoomLevels = [];
        for (var zl = 0; zl < bwg.numZoomLevels; ++zl) {
            var zlReduction = la[zl*6 + 16]
            var zlData = bwg_readOffset(ba, zl*24 + 72);
            var zlIndex = bwg_readOffset(ba, zl*24 + 80);
            bwg.zoomLevels.push({reduction: zlReduction, dataOffset: zlData, indexOffset: zlIndex});
        }

        return bwg;
      })
      .then(function(bwg) {
        return bwg._readChromTree();
      })
      .then(function(bwg) {
        return bwg._readAutoSQL();
      });
}

function BBISource() {

}

BBISource.prototype._readChromTree = function() {
    var thisB = this;
    this.chromsToIDs = {};
    this.idsToChroms = {};
    this.maxID = 0;

    var udo = this.unzoomedDataOffset;
    var eb = (udo - this.chromTreeOffset) & 3;
    udo = udo + 4 - eb;

    return this.data.slice(this.chromTreeOffset, udo - this.chromTreeOffset).fetch()
      .then(function(bpt) {
        var ba = new Uint8Array(bpt);
        var sa = new Int16Array(bpt);
        var la = new Int32Array(bpt);
        var bptMagic = la[0];
        var blockSize = la[1];
        var keySize = la[2];
        var valSize = la[3];
        var itemCount = bwg_readOffset(ba, 16);
        var rootNodeOffset = 32;

        var bptReadNode = function(offset) {
            var nodeType = ba[offset];
            var cnt = sa[(offset/2) + 1];
            offset += 4;
            for (var n = 0; n < cnt; ++n) {
                if (nodeType == 0) {
                    offset += keySize;
                    var childOffset = bwg_readOffset(ba, offset);
                    offset += 8;
                    childOffset -= thisB.chromTreeOffset;
                    bptReadNode(childOffset);
                } else {
                    var key = '';
                    for (var ki = 0; ki < keySize; ++ki) {
                        var charCode = ba[offset++];
                        if (charCode != 0) {
                            key += String.fromCharCode(charCode);
                        }
                    }
                    var chromId = (ba[offset+3]<<24) | (ba[offset+2]<<16) | (ba[offset+1]<<8) | (ba[offset+0]);
                    var chromSize = (ba[offset + 7]<<24) | (ba[offset+6]<<16) | (ba[offset+5]<<8) | (ba[offset+4]);
                    offset += 8;

                    thisB.chromsToIDs[key] = chromId;
                    if (key.indexOf('chr') == 0) {
                        thisB.chromsToIDs[key.substr(3)] = chromId;
                    }
                    thisB.idsToChroms[chromId] = key;
                    thisB.maxID = Math.max(thisB.maxID, chromId);
                }
            }
        };
        bptReadNode(rootNodeOffset);

        return thisB;
      });
}

BBISource.prototype._readAutoSQL = function() {
    return this;
}

BBISource.prototype.getUnzoomedView = function() {
    return new BBIView(this, this.unzoomedIndexOffset);
}

function BBIView(bbi, cirTreeOffset) {
    this.bbi = bbi;
    this.cirTreeOffset = cirTreeOffset;
}

BBIView.prototype.fetch = function(seqName, min, max) {
    var self = this;
    var chr = this.bbi.chromsToIDs[seqName];
    if (chr === undefined) {
        // Not an error because some files won't have data for all chromosomes.
        return Promise.resolve([]);
    } else {
        return self.readWigDataById(chr, min, max);
    }
}

BBIView.prototype.readWigDataById = function(chr, min, max) {
    var thisB = this;
    if (!this.cirHeader) {
        return this.bbi.data.slice(this.cirTreeOffset, 48).fetch()
          .then(function(result) {
            thisB.cirHeader = result;
            var la = new Int32Array(thisB.cirHeader);
            thisB.cirBlockSize = la[1];
            return thisB.readWigDataById(chr, min, max);
        });
        return;
    }

    return new Promise(function(resolve, reject) {
        var blocksToFetch = [];
        var outstanding = 0;

        var filter = function(chromId, fmin, fmax, toks) {
            return ((chr < 0 || chromId == chr) && fmin <= max && fmax >= min);
        }

        var cirFobRecur = function(offset, level) {
            outstanding += offset.length;

            if (offset.length == 1 && offset[0] - thisB.cirTreeOffset == 48 && thisB.cachedCirRoot) {
                cirFobRecur2(thisB.cachedCirRoot, 0, level);
                --outstanding;
                if (outstanding == 0) {
                    resolve(thisB.fetchFeatures(filter, blocksToFetch));
                }
                return;
            }

            var maxCirBlockSpan = 4 +  (thisB.cirBlockSize * 32);   // Upper bound on size, based on a completely full leaf node.
            var spans;
            for (var i = 0; i < offset.length; ++i) {
                var blockSpan = new Range(offset[i], offset[i] + maxCirBlockSpan);
                spans = spans ? union(spans, blockSpan) : blockSpan;
            }
            
            var fetchRanges = spans.ranges();
            for (var r = 0; r < fetchRanges.length; ++r) {
                var fr = fetchRanges[r];
                cirFobStartFetch(offset, fr, level);
            }
        }

        var cirFobStartFetch = function(offset, fr, level, attempts) {
            var length = fr.max() - fr.min();
            thisB.bbi.data.slice(fr.min(), fr.max() - fr.min()).fetch().
              then(function(resultBuffer) {
                for (var i = 0; i < offset.length; ++i) {
                    if (fr.contains(offset[i])) {
                        cirFobRecur2(resultBuffer, offset[i] - fr.min(), level);

                        if (offset[i] - thisB.cirTreeOffset == 48 && offset[i] - fr.min() == 0)
                            thisB.cachedCirRoot = resultBuffer;

                        --outstanding;
                        if (outstanding == 0) {
                            resolve(thisB.fetchFeatures(filter, blocksToFetch));
                        }
                    }
                }
            });
        }

        var cirFobRecur2 = function(cirBlockData, offset, level) {
            var ba = new Uint8Array(cirBlockData);
            var sa = new Int16Array(cirBlockData);
            var la = new Int32Array(cirBlockData);

            var isLeaf = ba[offset];
            var cnt = sa[offset/2 + 1];
            offset += 4;

            if (isLeaf != 0) {
                for (var i = 0; i < cnt; ++i) {
                    var lo = offset/4;
                    var startChrom = la[lo];
                    var startBase = la[lo + 1];
                    var endChrom = la[lo + 2];
                    var endBase = la[lo + 3];
                    var blockOffset = bwg_readOffset(ba, offset+16);
                    var blockSize = bwg_readOffset(ba, offset+24);
                    if (((chr < 0 || startChrom < chr) || (startChrom == chr && startBase <= max)) &&
                        ((chr < 0 || endChrom   > chr) || (endChrom == chr && endBase >= min)))
                    {
                        blocksToFetch.push({offset: blockOffset, size: blockSize});
                    }
                    offset += 32;
                }
            } else {
                var recurOffsets = [];
                for (var i = 0; i < cnt; ++i) {
                    var lo = offset/4;
                    var startChrom = la[lo];
                    var startBase = la[lo + 1];
                    var endChrom = la[lo + 2];
                    var endBase = la[lo + 3];
                    var blockOffset = bwg_readOffset(ba, offset+16);
                    if ((chr < 0 || startChrom < chr || (startChrom == chr && startBase <= max)) &&
                        (chr < 0 || endChrom   > chr || (endChrom == chr && endBase >= min)))
                    {
                        recurOffsets.push(blockOffset);
                    }
                    offset += 24;
                }
                if (recurOffsets.length > 0) {
                    cirFobRecur(recurOffsets, level + 1);
                }
            }
        };

        cirFobRecur([thisB.cirTreeOffset + 48], 1);
    });
}


BBIView.prototype.createFeature = function(chr, fmin, fmax, opts) {
    var f = {
        _chromId: chr,
        seqName: this.bbi.idsToChroms[chr],
        min: fmin,
        max: fmax
    };

    if (opts) {
        for (var k in opts) {
            f[k] = opts[k];
        }
    }
    return f;
}

BBIView.prototype.fetchFeatures = function(filter, blocks) {
    var thisB = this;

    blocks.sort(function(b0, b1) {
        return (b0.offset|0) - (b1.offset|0);
    });
    
    var blocksToFetch = [];
    if (blocks.length > 0) {
        var current = shallowCopy(blocks[0]);
        current.offsets = [current.offset];
        for (var bi = 1; bi < blocks.length; ++bi) {
            var b = blocks[bi];
            if (b.offset <= (current.offset + current.size)) {
                current.size = b.offset + b.size - current.offset;
                current.offsets.push(b.offset);

            } else {
                blocksToFetch.push(current);
                current = shallowCopy(b);
                current.offsets = [current.offset];
            }
        }
        blocksToFetch.push(current);
    }

    var features = [];
    var tramp = function(bi) {
        if (bi >= blocksToFetch.length) {
            return Promise.resolve(features);
        } else {
            var block = blocksToFetch[bi];
            return thisB.bbi.data.slice(block.offset, block.size).fetch()
              .then(function(result) {     
                for (var oi = 0; oi < block.offsets.length; ++oi) {
                    if (thisB.bbi.uncompressBufSize > 0) {
                        var data = jszlib_inflate_buffer(result, block.offsets[oi] - block.offset + 2);
                        thisB.parseFeatures(data, 0, data.byteLength, filter, features);
                    } else {
                        thisB.parseFeatures(
                            result, 
                            block.offsets[oi] - block.offset,
                            oi < block.offsets.length - 1 ? block.offsets[oi + 1] - block.offset : result.byteLength,
                            block.filter, 
                            features);
                    }
                }
                return tramp(bi + 1);
              });
        }
    };
    return tramp(0);
}

BBIView.prototype.parseFeatures = function(data, offset, limit, filter, features) {
    var ba = new Uint8Array(data, offset);

    if (this.isSummary) {
        var sa = new Int16Array(data, offset, ((limit - offset) / 2) | 0);
        var la = new Int32Array(data, offset, ((limit - offset) / 4) | 0);
        var fa = new Float32Array(data, offset, ((limit - offset) / 4) | 0);

        var itemCount = data.byteLength/32;
        for (var i = 0; i < itemCount; ++i) {
            var chromId =   la[(i*8)];
            var start =     la[(i*8)+1];
            var end =       la[(i*8)+2];
            var validCnt =  la[(i*8)+3];
            var minVal    = fa[(i*8)+4];
            var maxVal    = fa[(i*8)+5];
            var sumData   = fa[(i*8)+6];
            var sumSqData = fa[(i*8)+7];
            
            if (filter(chromId, start + 1, end)) {
                var summaryOpts = {type: 'bigwig', score: sumData/validCnt, maxScore: maxVal};
                if (this.bbi.type == 'bigbed') {
                    summaryOpts.type = 'density';
                }
                features.push(this.createFeature(chromId, start + 1, end, summaryOpts));
            }
        }
    } else if (this.bbi.type == 'bigwig') {
        var sa = new Int16Array(data, offset, ((limit - offset) / 2) | 0);
        var la = new Int32Array(data, offset, ((limit - offset) / 4) | 0);
        var fa = new Float32Array(data, offset, ((limit - offset) / 4) | 0);

        var chromId = la[0];
        var blockStart = la[1];
        var blockEnd = la[2];
        var itemStep = la[3];
        var itemSpan = la[4];
        var blockType = ba[20];
        var itemCount = sa[11];
        
        if (blockType == BIG_WIG_TYPE_FSTEP) {
            for (var i = 0; i < itemCount; ++i) {
                var score = fa[i + 6];
                var fmin = blockStart + (i*itemStep) + 1, fmax = blockStart + (i*itemStep) + itemSpan;
                if (filter(chromId, fmin, fmax))
                    features.push(this.createFeature(chromId, fmin, fmax, {score: score}));
            }
            return 24 + (itemCount * 4);
        } else if (blockType == BIG_WIG_TYPE_VSTEP) {
            for (var i = 0; i < itemCount; ++i) {
                var start = la[(i*2) + 6] + 1;
                var end = start + itemSpan - 1;
                var score = fa[(i*2) + 7];
                if (filter(chromId, start, end))
                    features.push(this.createFeature(chromId, start, end, {score: score}));
            }
            return 24 + (itemCount * 8);
        } else if (blockType == BIG_WIG_TYPE_GRAPH) {
            for (var i = 0; i < itemCount; ++i) {
                var start = la[(i*3) + 6] + 1;
                var end   = la[(i*3) + 7];
                var score = fa[(i*3) + 8];
                if (start > end) {
                    start = end;
                }
                if (filter(chromId, start, end))
                    features.push(this.createFeature(chromId, start, end, {score: score}));
            }
        } else {
            throw Error('Currently not handling bwgType=' + blockType);
        }
    } else if (this.bbi.type == 'bigbed') {
        limit -= offset;
        offset = 0;

        var dfc = this.bbi.definedFieldCount;
        var schema = this.bbi.schema;

        while (offset < ba.length) {
            var chromId = (ba[offset+3]<<24) | (ba[offset+2]<<16) | (ba[offset+1]<<8) | (ba[offset+0]);
            var start = (ba[offset+7]<<24) | (ba[offset+6]<<16) | (ba[offset+5]<<8) | (ba[offset+4]);
            var end = (ba[offset+11]<<24) | (ba[offset+10]<<16) | (ba[offset+9]<<8) | (ba[offset+8]);
            offset += 12;
            var rest = '';
            while (true) {
                var ch = ba[offset++];
                if (ch != 0) {
                    rest += String.fromCharCode(ch);
                } else {
                    break;
                }
            }

            var featureOpts = {};
            
            var bedColumns;
            if (rest.length > 0) {
                bedColumns = rest.split('\t');
            } else {
                bedColumns = [];
            }
            if (bedColumns.length > 0 && dfc > 3) {
                featureOpts.label = bedColumns[0];
            }
            if (bedColumns.length > 1 && dfc > 4) {
                var score = parseInt(bedColumns[1]);
                if (!isNaN(score))
                    featureOpts.score = score;
            }
            if (bedColumns.length > 2 && dfc > 5) {
                featureOpts.orientation = bedColumns[2];
            }
            if (bedColumns.length > 5 && dfc > 8) {
                var color = bedColumns[5];
                if (BED_COLOR_REGEXP.test(color)) {
                    featureOpts.itemRgb = 'rgb(' + color + ')';
                }
            }

            if (bedColumns.length > dfc-3 && schema) {
                for (var col = dfc - 3; col < bedColumns.length; ++col) {
                    featureOpts[schema.fields[col+3].name] = bedColumns[col];
                }
            }

            if (filter(chromId, start + 1, end, bedColumns)) {
                if (dfc < 12) {
                    features.push(this.createFeature(chromId, start + 1, end, featureOpts));
                } else {
                    var thickStart = bedColumns[3]|0;
                    var thickEnd   = bedColumns[4]|0;
                    var blockCount = bedColumns[6]|0;
                    var blockSizes = bedColumns[7].split(',');
                    var blockStarts = bedColumns[8].split(',');
                    
                    featureOpts.type = 'transcript';
                    var grp = {};
                    for (var k in featureOpts) {
                        grp[k] = featureOpts[k];
                    }
                    grp.id = bedColumns[0];
                    grp.segment = this.bbi.idsToChroms[chromId];
                    grp.min = start + 1;
                    grp.max = end;
                    grp.notes = [];
                    featureOpts.groups = [grp];

                    if (bedColumns.length > 9) {
                        var geneId = bedColumns[9];
                        var geneName = geneId;
                        if (bedColumns.length > 10) {
                            geneName = bedColumns[10];
                        }
                        var gg = shallowCopy(grp);
                        gg.id = geneId;
                        gg.label = geneName;
                        gg.type = 'gene';
                        featureOpts.groups.push(gg);
                    }

                    var spanList = [];
                    for (var b = 0; b < blockCount; ++b) {
                        var bmin = (blockStarts[b]|0) + start;
                        var bmax = bmin + (blockSizes[b]|0);
                        var span = new Range(bmin, bmax);
                        spanList.push(span);
                    }
                    var spans = union(spanList);
                    
                    var tsList = spans.ranges();
                    for (var s = 0; s < tsList.length; ++s) {
                        var ts = tsList[s];
                        features.push(this.createFeature(chromId, ts.min() + 1, ts.max(), featureOpts));
                    }

                    if (thickEnd > thickStart) {
                        var tl = intersection(spans, new Range(thickStart, thickEnd));
                        if (tl) {
                            featureOpts.type = 'translation';
                            var tlList = tl.ranges();
                            for (var s = 0; s < tlList.length; ++s) {
                                var ts = tlList[s];
                                features.push(this.createFeature(chromId, ts.min() + 1, ts.max(), featureOpts));
                            }
                        }
                    }
                }
            }
        }
    } else {
        throw Error("Don't know what to do with " + this.bbi.type);
    }
}

module.exports = {
    connectBBI: connectBBI
}