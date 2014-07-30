/* -*- mode: javascript; c-basic-offset: 4; indent-tabs-mode: nil -*- */


var bin = require('../src/binutils');

describe('URLFetchables', function() {
    var testURI = 'http://www.biodalliance.org/datasets/tests/test-leap.bb';
    var testFetchable;

    it('can be created from a URI', function() {
        window.f = new bin.URLFetchable(testURI);
    });

    it('can fetch data', function() {
        var data, err, flag;

        runs(function() {
            new bin.URLFetchable(testURI).fetch().then(
                function(result) {
                    data = result;
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
            expect(data).not.toBeNull();
            expect(data.byteLength).toBe(13013);
            var la = new Uint32Array(data, 0, 4);
            expect(la[0]).toBe(0x8789F2EB);
        });
    });
});
