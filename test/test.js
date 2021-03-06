var tilelive = require('tilelive');
var url = require('url');
var zlib = require('zlib');
var assert = require('assert');
var Vector = require('..');
var path = require('path');
var fs = require('fs');
var imageEqualsFile = require('./image.js');

// Load fixture data.
var xml = {
    a: fs.readFileSync(path.resolve(__dirname + '/test-a.xml'), 'utf8'),
    b: fs.readFileSync(path.resolve(__dirname + '/test-b.xml'), 'utf8'),
    c: fs.readFileSync(path.resolve(__dirname + '/test-c.xml'), 'utf8')
};
var infos = {
    a: { minzoom:0, maxzoom:1 },
    b: { minzoom:0, maxzoom:2, maskLevel:1 }
};
var tiles = {
    a: fs.readdirSync(path.resolve(__dirname + '/test-a')).reduce(function(memo, basename) {
        var key = basename.split('.').slice(0,3).join('.');
        memo[key] = fs.readFileSync(path.resolve(__dirname + '/test-a/' + basename));
        return memo;
    }, {}),
    b: fs.readdirSync(path.resolve(__dirname + '/test-b')).reduce(function(memo, basename) {
        var key = basename.split('.').slice(0,3).join('.');
        memo[key] = fs.readFileSync(path.resolve(__dirname + '/test-b/' + basename));
        return memo;
    }, {})
};

// Additional error tile fixtures.
zlib.deflate(new Buffer('asdf'), function(err, deflated) {
    if (err) throw err;
    tiles.a['1.0.2'] = new Buffer('asdf'); // invalid deflate
    tiles.a['1.0.3'] = deflated;           // invalid protobuf
});

var now = new Date;

// Tilelive test source.
tilelive.protocols['test:'] = Testsource;
function Testsource(uri, callback) {
    if (uri && uri.pathname) uri = uri.pathname.slice(1);

    this.uri = uri;
    if (uri) this.data = {
        minzoom: infos[uri].minzoom,
        maxzoom: infos[uri].maxzoom,
        maskLevel: infos[uri].maskLevel
    };
    this.stats = {};
    return callback && callback(null, this);
};
Testsource.prototype.getTile = function(z,x,y,callback) {
    var key = [z,x,y].join('.');

    // Count number of times each key is requested for tests.
    this.stats[key] = this.stats[key] || 0;
    this.stats[key]++;

    // Headers.
    var headers = {
        'Last-Modified': now.toUTCString(),
        'ETag':'73f12a518adef759138c142865287a18',
        'Content-Type':'application/x-protobuf'
    };

    if (!tiles[this.uri][key]) {
        return callback(new Error('Tile does not exist'));
    } else {
        return callback(null, tiles[this.uri][key], headers);
    }
};
Testsource.prototype.getInfo = function(callback) {
    return callback(null, this.data);
};


describe('tiles', function() {
    var sources = {
        a: new Vector({ backend: new Testsource('a'), xml: xml.a }),
        b: new Vector({ backend: new Testsource('b'), xml: xml.b }),
        'b@2x': new Vector({ backend: new Testsource('b'), xml: xml.b }),
        c: new Vector({ backend: new Testsource('b'), xml: xml.b, scale:2 }),
        d: new Vector({ backend: new Testsource('a'), xml: xml.a }),
        e: new Vector({ backend: new Testsource('a'), xml: xml.a, format:'png8:c=2' }),
        f: new Vector({ backend: new Testsource('a'), xml: xml.a.replace('png8:m=h', 'png8:c=2') }),
        g: new Vector({ backend: new Testsource('a'), xml: xml.a.replace('"scale">1', '"scale">2') })
    };
    var tests = {
        // 2.0.0, 2.0.1 test overzooming.
        // 1.1.2, 1.1.3 test that solid bg tiles are generated even when no
        // backend tile exists.
        a: ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1', '1.1.2', '1.1.3', '2.0.0', '2.0.1'],
        // 2.1.1 should use z2 vector tile -- a coastline shapefile
        // 2.1.2 should use maskLevel -- place dots, like the others
        b: ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1', '2.1.1', '2.1.2'],
        // test scale factor. unlike previous test, 3.2.2/3.2.3 will be coast
        // and 3.2.4 should fallback to the maskLevel
        c: ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1', '2.1.1', '2.1.2', '3.2.2', '3.2.3', '3.2.4'],
        // should match results for 'c' which has a 2x factor map object.
        'b@2x': ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1', '2.1.1', '2.1.2', '3.2.2', '3.2.3', '3.2.4'],
        // Checks for ETag stability.
        d: ['0.0.0', '1.0.0', '1.0.1', '1.1.0'],
        // Checks that explicit format in source URI overrides map parameters.
        e: ['0.0.0'],
        // Checks that format in map parameters beats default code fallback.
        f: ['0.0.0'],
        // Checks that scale in map parameters beats default code fallback.
        g: ['0.0.0']
    };
    var formats = {
        json: { ctype: 'application/json' },
        jpeg: { ctype: 'image/jpeg' },
        png: { ctype: 'image/png' },
        svg: { ctype: 'image/svg+xml' },
        utf: { ctype: 'application/json' }
    };
    var etags = {};
    Object.keys(tests).forEach(function(source) {
        before(function(done) { sources[source].open(done); });
    });
    Object.keys(tests).forEach(function(source) {
        tests[source].forEach(function(key) {
            var z = key.split('.')[0] | 0;
            var x = key.split('.')[1] | 0;
            var y = key.split('.')[2] | 0;
            var remaining = 2;
            it('should render ' + source + ' (' + key + ')', function(done) {
                var cbTile = function(err, buffer, headers) {
                    assert.ifError(err);
                    // No backend tiles last modified defaults to Date 0.
                    // Otherwise, Last-Modified from backend should be passed.
                    if (['1.1.2','1.1.3'].indexOf(key) >= 0) {
                        assert.equal(headers['Last-Modified'], new Date(0).toUTCString());
                    } else {
                        assert.equal(headers['Last-Modified'], now.toUTCString());
                    }
                    // Check for presence of ETag and store away for later
                    // ETag comparison.
                    assert.ok('ETag' in headers);
                    etags[source] = etags[source] || {};
                    etags[source][key] = headers['ETag'];
                    // Content-Type.
                    assert.equal(headers['Content-Type'], 'image/png');
                    // Load/draw stats attached to buffer.
                    assert.equal('number', typeof buffer._loadtime);
                    assert.equal('number', typeof buffer._drawtime);
                    // fs.writeFileSync(__dirname + '/expected/' + source + '.' + key + '.png', buffer);
                    imageEqualsFile(buffer, __dirname + '/expected/' + source + '.' + key + '.png', function(err) {
                        //assert.ifError(err);
                        if (!--remaining) done();
                    });
                };
                var cbHead = function(err, headers) {
                    assert.ifError(err);
                    // No backend tiles last modified defaults to Date 0.
                    // Otherwise, Last-Modified from backend should be passed.
                    if (['1.1.2','1.1.3'].indexOf(key) >= 0) {
                        assert.equal(headers['Last-Modified'], new Date(0).toUTCString());
                    } else {
                        assert.equal(headers['Last-Modified'], now.toUTCString());
                    }
                    // Content-Type.
                    assert.equal(undefined, headers['Content-Type']);
                    if (!--remaining) done();
                };
                if (/\@2x/.test(source)) {
                    cbTile.scale = 2;
                    cbHead.scale = 2;
                }
                sources[source].getTile(z,x,y, cbTile);
                sources[source].getHeaders(z,x,y, cbHead);
            });
        });
    });
    Object.keys(formats).forEach(function(format) {
        it('format a (0.0.0) as ' + format, function(done) {
            var source = 'a';
            var key = '0.0.0';
            var filepath = __dirname + '/expected/' + source + '.' + key + '.' + format;
            var cbTile = function(err, buffer, headers) {
                assert.ifError(err);
                assert.equal(headers['Content-Type'], formats[format].ctype);
                if (format === 'utf' || format === 'json') {
                    assert.deepEqual(buffer, JSON.parse(fs.readFileSync(filepath, 'utf8')));
                    done();
                    // fs.writeFileSync(filepath, JSON.stringify(buffer, null, 2));
                    // done();
                } else if (format === 'svg') {
                    assert.equal(buffer.length, fs.readFileSync(filepath).length);
                    done();
                } else {
                    imageEqualsFile(buffer, filepath, function(err) {
                        assert.ifError(err);
                        done();
                    });
                    // fs.writeFileSync(filepath, buffer);
                    // done();
                }
            };
            cbTile.format = format;
            sources[source].getTile(0,0,0, cbTile);
        });
    });
    it('errors out on bad deflate', function(done) {
        sources.a.getTile(1, 0, 2, function(err) {
            assert.equal('Z_DATA_ERROR', err.code);
            done();
        });
    });
    it('errors out on bad protobuf', function(done) {
        sources.a.getTile(1, 0, 3, function(err) {
            assert.equal('could not parse protobuf', err.message);
            done();
        });
    });
    it('same backend/xml => same ETags', function(done) {
        tests.a.slice(0,4).forEach(function(key) {
            assert.equal(etags.a[key], etags.d[key]);
        });
        done();    });
    it('diff blank tiles => diff ETags', function(done) {
        assert.notEqual(etags.a['1.1.2'], etags.a['1.1.3']);
        done();
    });
    it('diff backend => diff ETags', function(done) {
        tests.a.slice(0,4).forEach(function(key) {
            assert.notEqual(etags.a[key], etags.b[key]);
        });
        done();
    });
    it('diff scale => diff ETags', function(done) {
        tests.a.slice(0,4).forEach(function(key) {
            assert.notEqual(etags.b[key], etags.c[key]);
        });
        done();
    });
});

