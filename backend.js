var tilelive = require('tilelive');
var mapnik = require('mapnik');
var util = require('util');
var zlib = require('zlib');

module.exports = Backend;

function Task(callback) {
    this.err = null;
    this.headers = {};
    this.access = +new Date;
    this.done;
    this.body;
    this.once('done', callback);
};
util.inherits(Task, require('events').EventEmitter);

function Backend(opts, callback) {
    this._vectorCache = {};
    this._vectorTimeout = null;

    this._scale = opts.scale || 1;
    this._maxAge = typeof opts.maxAge === 'number' ? opts.maxAge : 60e3;
    this._deflate = typeof opts.deflate === 'boolean' ? opts.deflate : true;
    this._reap = typeof opts.reap === 'number' ? opts.reap : 60e3;

    // Properties derived/composed from sources.
    this._sources = [];
    this._infos = [];
    this._info;

    var backend = this;

    var uris = Array.isArray(opts.uri) ? opts.uri : opts.uri ? [opts.uri] : undefined;
    var sources = Array.isArray(opts.source) ? opts.source : opts.source ? [opts.source] : undefined;

    if (uris && sources) return callback(new Error('Only one of uri or source should be specified'));
    if (!uris && !sources) return callback(new Error('No sources found'));
    if (sources) this._sources = sources;

    var loaduris = function(queue, result, callback) {
        if (!queue) return callback();
        queue = queue.slice(0);
        queue.forEach(function(uri, i) { process.nextTick(function() {
            tilelive.load(uri, function(err, source) {
                if (err) return callback(err);
                result[i] = source;
                queue.shift();
                if (!queue.length) return callback();
            });
        })});
    };

    var loadinfo = function(queue, result, callback) {
        if (!queue) return callback();
        queue = queue.slice(0);
        queue.forEach(function(source, i) { process.nextTick(function() {
            source.getInfo(function(err, info) {
                if (err) return callback(err);
                result[i] = info;
                result[i].maxzoom = result[i].maxzoom || 22;
                result[i].minzoom = result[i].minzoom || 0;
                result[i].maskLevel = result[i].maskLevel || (source.data && source.data.maskLevel);
                result[i].vector_layers = result[i].vector_layers || [];
                queue.shift();
                if (!queue.length) return callback();
            });
        })});
    };

    loaduris(uris, backend._sources, function(err) {
        if (err) return callback(err);
        loadinfo(backend._sources, backend._infos, function(err) {
            if (err) return callback(err);

            // Single source does not require compositing.
            if (backend._sources.length === 1) {
                backend._info = backend._infos[0];
                return callback(null, backend);
            }

            // Combine all infos into a single info object.
            backend._info = backend._infos.reduce(function(memo, info) {
                return (Object.keys(info).reduce(function(memo, key) {
                    switch (key) {
                    case 'name':
                        memo[key] = memo[key] ? memo[key] + ' + ' + info[key] : info[key];
                        break;
                    case 'bounds':
                        memo[key] = memo[key] || [0,0,0,0];
                        memo[key][0] = Math.min(memo[key][0], info[key][0]);
                        memo[key][1] = Math.min(memo[key][1], info[key][1]);
                        memo[key][2] = Math.max(memo[key][2], info[key][2]);
                        memo[key][3] = Math.max(memo[key][3], info[key][3]);
                        break;
                    case 'minzoom':
                        memo[key] = Math.min(memo[key], info[key]);
                        break;
                    case 'maxzoom':
                        memo[key] = Math.max(memo[key], info[key]);
                        break;
                    case 'vector_layers':
                        memo[key] = memo[key].concat(info[key]);
                        break;
                    // Skipped keys.
                    case 'maskLevel':
                    case 'description':
                    case 'attribution':
                        break;
                    default:
                        memo[key] = memo[key] || info[key];
                        break;
                    }
                    return memo;
                }, memo));
            }, { minzoom:22, maxzoom:0, vector_layers:[] });

            // Compositing requires vector_layers.
            if (!backend._info.vector_layers.length)
                return callback(new Error('No vector_layers found on sources'));

            // Initialize compositing map.
            var map = new mapnik.Map(256,256);
            var xml = '<?xml version="1.0" encoding="utf-8"?>\n';
            xml += '<Map srs="+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over">\n';
            xml += backend._info.vector_layers.map(function(layer) {
                return '<Layer name="'+layer.id+'" buffer-size="256" srs="+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"></Layer>\n';
            }).join('');
            xml += '</Map>\n';
            map.bufferSize = 256;
            map.fromString(xml, function(err) {
                if (err) return callback(err);
                backend._map = map;
                backend._xml = xml;
                callback(null, backend);
            });
        });
    });
};

Backend.prototype.getInfo = function(callback) {
    return callback(null, this._info);
};

// Wrapper around backend.getTile that implements a "locking" cache.
Backend.prototype.getTile = function(z, x, y, callback) {
    var backend = this;

    // If scale > 1 adjusts source data zoom level inversely.
    // scale 2x => z-1, scale 4x => z-2, scale 8x => z-3, etc.
    var d = Math.round(Math.log(backend._scale)/Math.log(2));
    d = (z - d) > backend._info.minzoom ? d : 0;
    x = Math.floor(x / Math.pow(2, d));
    y = Math.floor(y / Math.pow(2, d));
    z = z - d;

    // Backend overzooming support.
    if (z > backend._info.maxzoom) {
        x = Math.floor(x / Math.pow(2, z - backend._info.maxzoom));
        y = Math.floor(y / Math.pow(2, z - backend._info.maxzoom));
        z = backend._info.maxzoom;
    }

    var key = z + '/' + x + '/' + y;
    var cache = backend._vectorCache[key];

    // Reap cached vector tiles with stale access times on an interval.
    if (backend._reap && !backend._vectorTimeout) backend._vectorTimeout = setTimeout(function() {
        var now = +new Date;
        Object.keys(backend._vectorCache).forEach(function(key) {
            if ((now - backend._vectorCache[key].access) < backend._maxAge) return;
            delete backend._vectorCache[key];
        });
        delete backend._vectorTimeout;
    }, backend._reap);

    // Expire cached tiles when they are past maxAge.
    var now = +new Date;
    if (cache && (now-cache.access) >= backend._maxAge) cache = false;

    // Return cache if finished.
    if (cache && cache.done) return callback(null, cache.body, cache.headers);

    // Otherwise add listener if task is in progress.
    if (cache) return cache.once('done', callback);

    var task = new Task(callback);
    backend._vectorCache[key] = task;

    var vtiles = [];
    var headers = {};
    var sources = backend._sources.slice(0);
    sources.forEach(function(source, idx) {
        var bz = z;
        var bx = x;
        var by = y;
        var size = 0;
        var maxz = backend._infos[idx] && (backend._infos[idx].maxzoom || 22);
        var mask = backend._infos[idx] && (backend._infos[idx].maskLevel);

        // Overzooming support.
        if (z > maxz) {
            bz = maxz;
            bx = Math.floor(x / Math.pow(2, z - bz));
            by = Math.floor(y / Math.pow(2, z - bz));
        }

        source.getTile(bz, bx, by, function sourceGet(err, body, head) {
            if (err && err.message === 'Tile does not exist' && mask && bz > mask) {
                bz = mask;
                bx = Math.floor(x / Math.pow(2, z - bz));
                by = Math.floor(y / Math.pow(2, z - bz));
                return source.getTile(bz, bx, by, sourceGet);
            }
            if (err && err.message !== 'Tile does not exist') return done(err);
            body = body || new Buffer(0);
            size = body.length;
            headers = composeHeaders(headers, head || {});
            return backend._deflate ? zlib.inflate(body, makevtile) : makevtile(null, body, b);
        });

        function makevtile(err, data) {
            if (err && err.message !== 'Tile does not exist') return done(err);
            var vtile = new mapnik.VectorTile(bz, bx, by);
            vtile._srcbytes = size;
            vtile.setData(data || new Buffer(0), function(err) {
                // Errors for null data are ignored as a solid tile should be painted.
                if (data && err) return done(err);
                vtiles[idx] = vtile;
                sources.shift();
                if (!sources.length) return done();
            });
        };
    });

    function composeHeaders(a, b) {
        var h = {};
        [a,b].forEach(function(s) {
            for (var k in s) {
                switch (k.toLowerCase()) {
                case 'etag':
                    h['ETag'] = (h['ETag'] ? h['ETag'] + '-' : '') + s[k];
                    break;
                case 'last-modified':
                    h['Last-Modified'] = new Date(Math.max(
                        new Date(h['Last-Modified']||0),
                        new Date(s[k])
                    )).toUTCString();
                    break;
                case 'content-type':
                    h['Content-Type'] = h['Content-Type'] || s[k];
                    break;
                }
            };
        });
        return h;
    };

    function done(err) {
        if (err) return (function error(err) {
            delete backend._vectorCache[key];
            task.done = true;
            return task.emit('done', err);
        })(err);

        if (vtiles.length === 1) {
            task.done = true;
            task.body = vtiles[0];
            task.headers = headers;
            return task.emit('done', err, task.body, task.headers);
        }

        mapnik.render(z,x,y, backend._map, new mapnik.VectorTile(z,x,y), vtiles, {}, function(err, surface) {
            if (err) return error(err);
            task.done = true;
            task.body = surface;
            task.headers = headers;
            return task.emit('done', err, task.body, task.headers);
        });
    };
};

