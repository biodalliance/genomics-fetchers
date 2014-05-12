"use strict";

// Imports

var Promise = require('es6-promise').Promise;

function BlobFetchable(b) {
    this.blob = b;
}

BlobFetchable.prototype.slice = function(start, length) {
    var b;

    if (this.blob.slice) {
        if (length) {
            b = this.blob.slice(start, start + length);
        } else {
            b = this.blob.slice(start);
        }
    } else {
        if (length) {
            b = this.blob.webkitSlice(start, start + length);
        } else {
            b = this.blob.webkitSlice(start);
        }
    }
    return new BlobFetchable(b);
}

if (typeof(FileReaderSync) !== 'undefined') {
    BlobFetchable.prototype.fetch = function() {
        return new Promise(function(resolve, reject) {
            var reader = new FileReaderSync();
            resolve(reader.readAsArrayBuffer(this.blob));
        });
    }
} else {
    BlobFetchable.prototype.fetch = function() {
        return new Promise(function(resolve, reject) {
            var reader = new FileReader();
            reader.onloadend = function(ev) {
                if (reader.error)
                    reject(reader.error)
                else
                    resolve(reader.result);
            };
            reader.readAsArrayBuffer(this.blob);
        });
    }
}

function URLFetchable(url, start, end, opts) {
    if (!opts) {
        if (typeof start === 'object') {
            opts = start;
            start = undefined;
        } else {
            opts = {};
        }
    }

    this.url = url;
    this.start = start || 0;
    if (end) {
        this.end = end;
    }
    this.opts = opts;
}

URLFetchable.prototype.slice = function(s, l) {
    if (s < 0) {
        throw Error('Bad slice ' + s);
    }

    var ns = this.start, ne = this.end;
    if (ns && s) {
        ns = ns + s;
    } else {
        ns = s || ns;
    }
    if (l && ns) {
        ne = ns + l - 1;
    } else {
        ne = ne || l - 1;
    }
    return new URLFetchable(this.url, ns, ne, this.opts);
}

/*
URLFetchable.prototype.fetchAsText = function(callback) {
    var req = new XMLHttpRequest();
    var length;
    var url = this.url;
    req.open('GET', url, true);

    if (this.end) {
        if (this.end - this.start > 100000000) {
            throw 'Monster fetch!';
        }
        req.setRequestHeader('Range', 'bytes=' + this.start + '-' + this.end);
        length = this.end - this.start + 1;
    }

    req.onreadystatechange = function() {
        if (req.readyState == 4) {
            if (req.status == 200 || req.status == 206) {
                return callback(req.responseText);
            } else {
                return callback(null);
            }
        }
    };
    if (this.opts.credentials) {
        req.withCredentials = true;
    }
    req.send('');
} */

URLFetchable.prototype.fetch = function() {
    var self = this;

    return new Promise(function(resolve, reject) {
        var req = new XMLHttpRequest();
        var url = self.url;
        req.open('GET', url, true);
        req.overrideMimeType('text/plain; charset=x-user-defined');
        if (self.end) {
            if (self.end - self.start > 100000000) {
                throw Error('Monster fetch!');
            }
            req.setRequestHeader('Range', 'bytes=' + self.start + '-' + self.end);
        }
        req.responseType = 'arraybuffer';
        req.onreadystatechange = function() {
            if (req.readyState == 4) {
                if (req.status == 200 || req.status == 206) {
                    if (req.response) {
                        resolve(req.response);
                    } else if (req.mozResponseArrayBuffer) {
                        resolve(req.mozResponseArrayBuffer);
                    } else {
                        resolve(bstringToBuffer(req.responseText));
                    }
                } else {
                    reject('status=' + req.status);
                }
            }
        };
        if (self.opts.credentials) {
            req.withCredentials = true;
        }
        req.send('');
    });
};

// Read from Uint8Array

(function(global) {
    var convertBuffer = new ArrayBuffer(8);
    var ba = new Uint8Array(convertBuffer);
    var fa = new Float32Array(convertBuffer);

    global.readFloat = function(buf, offset) {
        ba[0] = buf[offset];
        ba[1] = buf[offset+1];
        ba[2] = buf[offset+2];
        ba[3] = buf[offset+3];
        return fa[0];
    };
 }(this));

function readInt64(ba, offset) {
    return (ba[offset + 7] << 24) | (ba[offset + 6] << 16) | (ba[offset + 5] << 8) | (ba[offset + 4]);
}

function readInt(ba, offset) {
    return (ba[offset + 3] << 24) | (ba[offset + 2] << 16) | (ba[offset + 1] << 8) | (ba[offset]);
}

function readShort(ba, offset) {
    return (ba[offset + 1] << 8) | (ba[offset]);
}

function readByte(ba, offset) {
    return ba[offset];
}

function readIntBE(ba, offset) {
    return (ba[offset] << 24) | (ba[offset + 1] << 16) | (ba[offset + 2] << 8) | (ba[offset + 3]);
}

// Exports if we are being used as a module

if (typeof(module) !== 'undefined') {
    module.exports = {
        BlobFetchable: BlobFetchable,
        URLFetchable: URLFetchable,

        readInt: readInt,
        readIntBE: readIntBE,
        readInt64: readInt64,
        readShort: readShort,
        readByte: readByte,
        readFloat: this.readFloat
    }
}