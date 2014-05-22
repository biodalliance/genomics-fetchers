/* -*- mode: javascript; c-basic-offset: 4; indent-tabs-mode: nil -*- */


var bbi = require('../src/bbi');
var bin = require('../src/binutils');

describe('BBISource objects', function() {
    var testURI = 'http://www.biodalliance.org/datasets/tests/test-leap.bb';

    it('Can be created from a URL', function() {
        var bbif, err, flag;

        runs(function() {
            bbi.connectBBI(new bin.URLFetchable(testURI)).then(
                function(result) {
                    bbif = result;
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
            expect(bbif).not.toBeNull();
            console.log(bbif.type);
            console.log(bbif.chromsToIDs);
        });
    });
});