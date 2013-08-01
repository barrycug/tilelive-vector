var mapnik = require('mapnik');
var path = require('path');
var util = require('util');
var crypto = require('crypto');
var Backend = require('./backend');

module.exports = Vector;

function Vector(uri, callback) {
    if (!uri.xml) return callback && callback(new Error('No xml'));

    this._uri = uri;
    this._scale = uri.scale || undefined;
    this._format = uri.format || undefined;
    this._source = uri.source || undefined;
    this._base = path.resolve(uri.base || __dirname);

    if (callback) this.once('open', callback);

    this.update(uri, function(err) {
        this.emit('open', err, this);
    }.bind(this));
};
util.inherits(Vector, require('events').EventEmitter);

// Helper for callers to ensure source is open. This is not built directly
// into the constructor because there is no good auto cache-keying system
// for these tile sources (ie. sharing/caching is best left to the caller).
Vector.prototype.open = function(callback) {
    if (this._map) return callback(null, this);
    this.once('open', callback);
};

// Allows in-place update of XML/backends.
Vector.prototype.update = function(opts, callback) {
    // If the XML has changed update the map.
    if (!opts.xml || this._xml === opts.xml) return callback();

    var map = new mapnik.Map(256,256);
    map.fromString(opts.xml, {
        strict: false,
        base: this._base + '/'
    }, function(err) {
        delete this._info;
        this._xml = opts.xml;
        this._map = map;
        this._md5 = crypto.createHash('md5').update(opts.xml).digest('hex');
        this._format = opts.format || map.parameters.format || this._format || 'png8:m=h';
        this._scale = opts.scale || +map.parameters.scale || this._scale || 1;
        map.bufferSize = 256 * this._scale;

        var source = map.parameters.source || opts.source;
        if (!this._backend || this._source !== source) {
            if (!source) return callback(new Error('No backend'));
            new Backend({
                uri: [source],
                // Unlike other opts, backend scale can be influenced by
                // the map style so it is not derived from this.uri.
                scale: this._scale,
                reap: this._uri.reap,
                maxAge: this._uri.maxAge,
                deflate: this._uri.deflate
            }, function(err, backend) {
                if (err) return callback(err);
                this._source = map.parameters.source || opts.source;
                this._backend = backend;
                return callback();
            }.bind(this));
        } else {
            return callback();
        }
    }.bind(this));
    return;
};

Vector.prototype.getTile = function(z, x, y, callback) {
    if (!this._map) return callback(new Error('Tilesource not loaded'));

    // Hack around tilelive API - allow params to be passed per request
    // as attributes of the callback function.
    var format = callback.format || this._format || this._map.parameters.format || 'png8:m=h';

    var source = this;
    var drawtime;
    var loadtime = +new Date;
    this._backend.getTile(z, x, y, function(err, vtile, head) {
        if (err && err.message !== 'Tile does not exist')
            return callback(err);

// @TODO
//        if (err && source._maskLevel && bz > source._maskLevel)
//            return callback(format === 'utf' ? new Error('Grid does not exist') : err);

        var headers = {};
        switch (format.match(/^[a-z]+/i)[0]) {
        case 'headers':
            // No content type for header-only.
            break;
        case 'json':
        case 'utf':
            headers['Content-Type'] = 'application/json';
            break;
        case 'jpeg':
            headers['Content-Type'] = 'image/jpeg';
            break;
        case 'svg':
            headers['Content-Type'] = 'image/svg+xml';
            break;
        case 'png':
        default:
            headers['Content-Type'] = 'image/png';
            break;
        }
        headers['ETag'] = JSON.stringify(crypto.createHash('md5')
            .update(source._scale + source._md5 + (head && head['ETag'] || (z+','+x+','+y)))
            .digest('hex'));
        headers['Last-Modified'] = new Date(head && head['Last-Modified'] || 0).toUTCString();

        // Return headers for 'headers' format.
        if (format === 'headers') return callback(null, headers, headers);

        loadtime = (+new Date) - loadtime;
        drawtime = +new Date;

        var opts = {z:z, x:x, y:y, scale:source._scale};
        if (format === 'json') {
            try { return callback(null, vtile.toJSON(), headers); }
            catch(err) { return callback(err); }
        } else if (format === 'utf') {
            var surface = new mapnik.Grid(256,256);
            opts.layer = source._map.parameters.interactivity_layer;
            opts.fields = source._map.parameters.interactivity_fields.split(',');
        } else if (format === 'svg') {
            var surface = new mapnik.CairoSurface('svg',256,256);
        } else {
            var surface = new mapnik.Image(256,256);
        }
        vtile.render(source._map, surface, opts, function(err, image) {
            if (err) return callback(err);
            if (format == 'svg') {
                headers['Content-Type'] = 'image/svg+xml';
                return callback(null, image.getData(), headers);
            } else if (format === 'utf') {
                image.encode(format, {}, function(err, buffer) {
                    if (err) return callback(err);
                    return callback(null, buffer, headers);
                });
            } else {
                image.encode(format, {}, function(err, buffer) {
                    if (err) return callback(err);
                    buffer._loadtime = loadtime;
                    buffer._drawtime = (+new Date) - drawtime;
                    buffer._srcbytes = vtile._srcbytes || 0;
                    return callback(null, buffer, headers);
                });
            }
        });
    });
};

Vector.prototype.getGrid = function(z, x, y, callback) {
    if (!this._map) return callback(new Error('Tilesource not loaded'));
    if (!this._map.parameters.interactivity_layer) return callback(new Error('Tilesource has no interactivity_layer'));
    if (!this._map.parameters.interactivity_fields) return callback(new Error('Tilesource has no interactivity_fields'));
    callback.format = 'utf';
    return this.getTile(z, x, y, callback);
};

Vector.prototype.getHeaders = function(z, x, y, callback) {
    callback.format = 'headers';
    return this.getTile(z, x, y, callback);
};

Vector.prototype.getInfo = function(callback) {
    if (!this._map) return callback(new Error('Tilesource not loaded'));
    if (this._info) return callback(null, this._info);

    var params = this._map.parameters;
    this._info = Object.keys(params).reduce(function(memo, key) {
        switch (key) {
        // The special "json" key/value pair allows JSON to be serialized
        // and merged into the metadata of a mapnik XML based source. This
        // enables nested properties and non-string datatypes to be
        // captured by mapnik XML.
        case 'json':
            try { var jsondata = JSON.parse(params[key]); }
            catch (err) { return callback(err); }
            Object.keys(jsondata).reduce(function(memo, key) {
                memo[key] = memo[key] || jsondata[key];
                return memo;
            }, memo);
            break;
        case 'bounds':
        case 'center':
            memo[key] = params[key].split(',').map(function(v) { return parseFloat(v) });
            break;
        case 'minzoom':
        case 'maxzoom':
            memo[key] = parseInt(params[key], 10);
            break;
        default:
            memo[key] = params[key];
            break;
        }
        return memo;
    }, {});
    return callback(null, this._info);
};

