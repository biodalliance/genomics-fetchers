/* -*- mode: javascript; c-basic-offset: 4; indent-tabs-mode: nil -*- */


var bbi = require('../src/bbi');
var bin = require('../src/binutils');

describe('BBISource objects', function() {
    var testURI = 'http://www.biodalliance.org/datasets/tests/test-leap.bb';
    var bb;

    it('Can be created from a URL', function() {
        var bbif, err, flag;

        runs(function() {
            bbi.connectBBI(new bin.URLFetchable(testURI)).then(
                function(result) {
                    bb = result;
                    flag = true;
                },
                function(error) {
                    console.log(error);
                    err = error;
                    flag = true;
                }
            );
        });

        waitsFor(function() {
            return flag;
        }, 'Expects callback after fetch');

        runs(function() {
            expect(err).toBeFalsy();
            expect(bb).not.toBeNull();
            console.log(bb.type);
            console.log(bb.chromsToIDs);
        });
    });

    it('can retrieve features from a genomic interval', function() {
        var features, err, flag;

        runs(function() {
            bb.getUnzoomedView().fetch('chr1', 1, 100000000).then(
                function(result) {
                    console.log(result);
                    features = result;
                    flag = true;
                },
                function(error) {
                    console.log(error);
                    console.log(error.stack);
                    err = error;
                    flag = true;
                });
        });

        waitsFor(function() {
            return flag;
        }, 'Expects callback after feature fetch');

        runs(function() {
            expect(err).toBeFalsy();
            expect(features).toBeTruthy();
            expect(features.length > 0).toBeTruthy();
        });
    });
});