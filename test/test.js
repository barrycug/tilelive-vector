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
    a: { name:'coastline', minzoom:0, maxzoom:1, vector_layers:[{id:'coastline'}], bounds:[-180,-60,180,60] },
    b: { name:'places', minzoom:0, maxzoom:2, maskLevel:1, vector_layers:[{id:'places'}], bounds:[-175,-85.0511,175,75] }
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
        name: infos[uri].name,
        minzoom: infos[uri].minzoom,
        maxzoom: infos[uri].maxzoom,
        maskLevel: infos[uri].maskLevel,
        vector_layers: infos[uri].vector_layers,
        bounds: infos[uri].bounds
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

describe('init', function() {
    it('should fail without backend', function(done) {
        new Vector({ xml: xml.c }, function(err) {
            assert.equal(err.message, 'No backend');
            done();
        });
    });
    it('should fail without xml', function(done) {
        new Vector({ backend: new Testsource() }, function(err) {
            assert.equal(err.message, 'No xml');
            done();
        });
    });
    it('should load with callback', function(done) {
        new Vector({ backend: new Testsource(), xml: xml.a }, function(err, source) {
            assert.ifError(err);
            assert.ok(source);
            done();
        });
    });
    it('#open should call all listeners', function(done) {
        var v = new Vector({ backend: new Testsource(), xml: xml.a });
        var remaining = 3;
        for (var i = 0; i < remaining; i++) v.open(function(err, source) {
            assert.ifError(err);
            assert.ok(source);
            if (!--remaining) done();
        });
    });
    it('should get info', function(done) {
        new Vector({ backend: new Testsource(), xml: xml.a }, function(err, source) {
            assert.ifError(err);
            assert.ok(source);
            source.getInfo(function(err, info) {
                assert.ifError(err);
                assert.equal('test-a', info.name);
                assert.equal(0, info.minzoom);
                assert.equal(8, info.maxzoom);
                assert.deepEqual([0,0,2], info.center);
                assert.deepEqual([-180,-85.0511,180,85.0511], info.bounds);
                assert.deepEqual({"level2":"property"}, info.level1, 'JSON key stores deep attribute data');
                assert.deepEqual(1, info.scale, 'JSON key does not overwrite other params');
                done();
            });
        });
    });
    it('should update xml, backend', function(done) {
        new Vector({xml:xml.a}, function(err, source) {
            assert.ifError(err);
            assert.equal('a',source._backend._sources[0].uri);
            source.getInfo(function(err, info) {
                assert.ifError(err);
                assert.equal('test-a', info.name);
                source.update({xml:xml.b}, function(err) {
                    assert.ifError(err);
                    source.getInfo(function(err, info) {
                        assert.ifError(err);
                        assert.equal('test-b', info.name);
                        assert.equal('b',source._backend._sources[0].uri);
                        done();
                    });
                });
            });
        });
    });
    it('should use fallback backend', function(done) {
        new Vector({ source:'test:///a', xml: xml.c }, function(err, source) {
            assert.ifError(err);
            assert.ok(source);
            assert.equal('a',source._backend._sources[0].uri);
            done();
        });
    });
});

describe('backend', function() {
    it('should error without uris/sources', function(done) {
        new Vector.Backend({}, function(err, backend) {
            assert.equal('No sources found', err.message);
            done();
        });
    });
    it('should error with both uris/sources', function(done) {
        new Vector.Backend({
            uri: 'test:///a',
            source: new Testsource('a')
        }, function(err, backend) {
            assert.equal('Only one of uri or source should be specified', err.message);
            done();
        });
    });
    [ ['uri', { uri: 'test:///a' }],
      ['source', { source: new Testsource('a') }]
    ].forEach(function(test) {
        var label = test[0];
        var opts = test[1];
        it('should load from single ' + label, function(done) {
            new Vector.Backend(opts, function(err, backend) {
                assert.ifError(err);
                assert.equal(1, backend._sources.length);
                assert.equal(1, backend._infos.length);
                backend.getInfo(function(err, info) {
                    assert.ifError(err);
                    assert.deepEqual(info, {
                        name: 'coastline',
                        minzoom: 0,
                        maxzoom: 1,
                        maskLevel: undefined,
                        bounds: [-180,-60,180,60],
                        vector_layers: [ { id: 'coastline' } ]
                    });
                    done();
                });
            });
        });
    });
    [ ['uris', { uri: ['test:///a','test:///b'] }],
      ['sources', { source: [new Testsource('a'),new Testsource('b')] }]
    ].forEach(function(test) {
        var label = test[0];
        var opts = test[1];
        it('should composite multiple ' + label, function(done) {
            new Vector.Backend({ uri: ['test:///a','test:///b'] }, function(err, backend) {
                assert.ifError(err);
                assert.equal(2, backend._sources.length);
                assert.equal(2, backend._infos.length);
                backend.getInfo(function(err, info) {
                    assert.ifError(err);
                    assert.deepEqual(info, {
                        name: 'coastline + places',
                        minzoom: 0,
                        maxzoom: 2,
                        bounds: [-180,-85.0511,180,75],
                        vector_layers: [ { id: 'coastline' }, { id: 'places' } ],
                    });
                    done();
                });
            });
        });
    });
});

describe('tiles', function() {
    var sources = {
        a: new Vector({ xml: xml.a }),
        b: new Vector({ xml: xml.b }),
        c: new Vector({ xml: xml.b, scale:2 }),
        d: new Vector({ xml: xml.a }),
        e: new Vector({ xml: xml.a, format:'png8:c=2' }),
        f: new Vector({ xml: xml.a.replace('png8:m=h', 'png8:c=2') }),
        g: new Vector({ xml: xml.a.replace('"scale">1', '"scale">2') }),
        h: new Vector({ source:['test:///a', 'test:///b'], xml: xml.c }),
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
        // Checks for ETag stability.
        d: ['0.0.0', '1.0.0', '1.0.1', '1.1.0'],
        // Checks that explicit format in source URI overrides map parameters.
        e: ['0.0.0'],
        // Checks that format in map parameters beats default code fallback.
        f: ['0.0.0'],
        // Checks that scale in map parameters beats default code fallback.
        g: ['0.0.0'],
        // Compositing. See notes for a + b above.
        h: ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1', '1.1.2', '1.1.3', '2.0.0', '2.0.1', '2.1.1', '2.1.2']
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
                sources[source].getTile(z,x,y, function(err, buffer, headers) {
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
                    imageEqualsFile(buffer, __dirname + '/expected/' + source + '.' + key + '.png', function(err) {
                        assert.ifError(err);
                        if (!--remaining) done();
                    });
                    // fs.writeFileSync(__dirname + '/expected/' + source + '.' + key + '.png', buffer);
                });
                sources[source].getHeaders(z,x,y, function(err, headers) {
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
                });
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
    it('same sources => same ETags', function(done) {
        tests.a.slice(0,4).forEach(function(key) {
            assert.equal(etags.a[key], etags.d[key]);
        });
        done();    });
    it('diff blank tiles => diff ETags', function(done) {
        assert.notEqual(etags.a['1.1.2'], etags.a['1.1.3']);
        done();
    });
    it('diff sources => diff ETags', function(done) {
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

describe('cache', function() {
    var source = new Vector({
        backend: new Testsource('a'),
        xml: xml.a,
        maxAge: 1000
    });
    var requests = ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1', '2.0.0', '2.0.1'];
    before(function(done) { source.open(done); });
    requests.forEach(function(key) {
        var z = key.split('.')[0] | 0;
        var x = key.split('.')[1] | 0;
        var y = key.split('.')[2] | 0;
        before(function(done) {
            // Request each tile twice.
            source.getTile(z, x, y, function(err, buffer, headers) {
                assert.ifError(err);
                source.getTile(z, x, y, function(err, buffer, headers) {
                    assert.ifError(err);
                    done();
                });
            });
        });
    });
    it('lockingcache should singleton requests to backend', function(done) {
        assert.equal(source._backend._sources[0].stats['0.0.0'], 1);
        assert.equal(source._backend._sources[0].stats['1.0.0'], 1);
        assert.equal(source._backend._sources[0].stats['1.0.1'], 1);
        assert.equal(source._backend._sources[0].stats['1.1.0'], 1);
        assert.equal(source._backend._sources[0].stats['1.1.1'], 1);
        assert.equal(source._backend._sources[0].stats['2.0.0'], undefined);
        assert.equal(source._backend._sources[0].stats['2.0.1'], undefined);
        done();
    });
    it('cached tiles should expire after maxAge', function(done) {
        source.getTile(0, 0, 0, function(err, buffer, headers) {
            assert.ifError(err);
            setTimeout(function() {
                source.getTile(1, 0, 0, function(err, buffer, headers) {
                    assert.ifError(err);
                    assert.equal(source._backend._sources[0].stats['0.0.0'], 1);
                    assert.equal(source._backend._sources[0].stats['1.0.0'], 2);
                    done();
                });
            }, 1000);
        });
    });
});

describe('reap', function() {
    var source = new Vector({
        backend: new Testsource('a'),
        xml: xml.a,
        maxAge: 1000,
        reap: 500
    });
    var requests = ['0.0.0', '1.0.0', '1.0.1', '1.1.0', '1.1.1'];
    before(function(done) { source.open(done); });
    requests.forEach(function(key) {
        var z = key.split('.')[0] | 0;
        var x = key.split('.')[1] | 0;
        var y = key.split('.')[2] | 0;
        before(function(done) {
            source.getTile(z, x, y, function(err, buffer, headers) {
                assert.ifError(err);
                done();
            });
        });
    });
    it('backend should have a populated cache', function(done) {
        assert.equal(Object.keys(source._backend._vectorCache).length, 5);
        done();
    });
    it('backend should reap expired tiles', function(done) {
        setTimeout(function() {
            source.getTile(0, 0, 0, function(err, buffer, headers) {
                assert.ifError(err);
                setTimeout(function() {
                    assert.equal(Object.keys(source._backend._vectorCache).length, 0);
                    done();
                }, 500);
            });
        }, 500);
    });
});
