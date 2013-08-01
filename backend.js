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
    this._sources = [];
    this._scale = opts.scale || 1;
    this._maxAge = typeof opts.maxAge === 'number' ? opts.maxAge : 60e3;
    this._deflate = typeof opts.deflate === 'boolean' ? opts.deflate : true;
    this._reap = typeof opts.reap === 'number' ? opts.reap : 60e3;
    var backend = this;
    var sources = this._sources;

    var uris = Array.isArray(opts.uri) ? opts.uri.slice(0) : [opts.uri];
    uris.forEach(function(uri, i) {
        tilelive.load(uri, function(err, source) {
            if (err) return callback(err);
            source.getInfo(function(err, info) {
                if (err) return callback(err);
                source._minzoom = info.minzoom || 0;
                source._maxzoom = info.maxzoom || 22;
                // @TODO some sources filter out custom keys @ getInfo forcing
                // access to info/data properties directly. Fix this.
                source._maskLevel = ('maskLevel' in info)
                    ? parseInt(info.maskLevel, 10)
                    : (source.data && 'maskLevel' in source.data)
                    ? source.data.maskLevel
                    : undefined;
                sources[i] = source;
                uris.shift();
                if (!uris.length) return callback(null, backend);
            });
        });
    });
};

// Wrapper around backend.getTile that implements a "locking" cache.
Backend.prototype.getTile = function(z, x, y, callback) {
    var backend = this;
    var now = +new Date;
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
    if (cache && (now-cache.access) >= backend._maxAge) cache = false;

    // Return cache if finished.
    if (cache && cache.done) return callback(null, cache.body, cache.headers);

    // Otherwise add listener if task is in progress.
    if (cache) return cache.once('done', callback);

    var task = new Task(callback);
    backend._vectorCache[key] = task;

    // @TODO.
    var size = 0;
    var headers = {};

    // @TODO support multiple sources here.
    // Currently hardcoded to a single source.
    var source = backend._sources[0];

    // If scale > 1 adjusts source data zoom level inversely.
    // scale 2x => z-1, scale 4x => z-2, scale 8x => z-3, etc.
    var d = Math.round(Math.log(this._scale)/Math.log(2));
    var bz = (z - d) > source._minzoom ? z - d : source._minzoom;
    var bx = Math.floor(x / Math.pow(2, z - bz));
    var by = Math.floor(y / Math.pow(2, z - bz));

    // Overzooming support.
    if (bz > source._maxzoom) {
        bz = source._maxzoom;
        bx = Math.floor(x / Math.pow(2, z - bz));
        by = Math.floor(y / Math.pow(2, z - bz));
    }

    // @TODO maskLevel support (per source) right here!
    source.getTile(bz, bx, by, function sourceGet(err, body, head) {
        if (typeof source._maskLevel === 'number' &&
            err && err.message === 'Tile does not exist' &&
            bz > source._maskLevel) {
            bz = source._maskLevel;
            bx = Math.floor(x / Math.pow(2, z - bz));
            by = Math.floor(y / Math.pow(2, z - bz));
            return source.getTile(bz, bx, by, sourceGet);
        }
        if (err && err.message !== 'Tile does not exist') return done(err);
        body = body || new Buffer(0);
        size = body.length;
        headers = head || {};
        return backend._deflate ? zlib.inflate(body, makevtile) : makevtile(null, body, b);
    });

    function done(err, body, headers) {
        if (err) delete backend._vectorCache[key];
        task.done = true;
        task.body = body;
        task.headers = headers;
        task.emit('done', err, body, headers);
    };
    function makevtile(err, data) {
        if (err && err.message !== 'Tile does not exist') return done(err);
        var vtile = new mapnik.VectorTile(bz, bx, by);
        vtile._srcbytes = size;
        vtile.setData(data || new Buffer(0), function(err) {
            // Errors for null data are ignored as a solid tile be painted.
            if (data && err) return done(err);
            return done(err, vtile, headers);
        });
    };
};

