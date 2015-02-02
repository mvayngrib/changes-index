var chproc = require('level-change-processor');
var sublevel = require('subleveldown');
var isarray = require('isarray');
var inherits = require('inherits');
var EventEmitter = require('events').EventEmitter;
var changesdown = require('changesdown');
var defined = require('defined');
var through = require('through2');
var bytewise = require('bytewise');
var wrap = require('level-option-wrap');

module.exports = Ix;
inherits(Ix, EventEmitter);

function Ix (opts) {
    if (!(this instanceof Ix)) return new Ix(opts);
    EventEmitter.call(this);
    if (!opts) opts = {};
    this.ixdb = opts.ixdb;
    this.chdb = opts.chdb;
    
    this.rdb = sublevel(this.ixdb, 'r', { keyEncoding: bytewise });
    this.cdb = sublevel(this.ixdb, 'c');
    
    this.feed = opts.feed;
    this.names = {};
}

Ix.prototype.add = function (fn) {
    var self = this;
    var proc = chproc({
        db: self.cdb,
        feed: self.feed,
        worker: worker
    });
    
    proc.on('error', function (err) {
        self.emit('error', err);
    });
    
    function worker (ch, cb) {
        var rows = self._decode(ch.value);
        (function next (err) {
            if (err) return cb(err);
            if (rows.length === 0) return cb();
            var row = rows.shift();
            fn(row, function (err, indexes) {
                if (err) return cb(err);
                if (!indexes) return next();
                if (typeof indexes !== 'object') {
                    return cb(new Error('object expected for the indexes'));
                }
                var batch = Object.keys(indexes).map(onmap);
                if (batch.length === 0) return next();
                self.rdb.batch(batch, next);
                
                function onmap (key) {
                    return {
                        type: row.type,
                        key: [ key, indexes[key], row.rawKey.toString('hex') ],
                        value: '0'
                    };
                }
            });
        })();
    }
};

Ix.prototype.exists = function (name, key, cb) {
    var opts = {
        gte: [ name, key, null ],
        lte: [ name, key, undefined ]
    };
    var r = this.rdb.createReadStream(opts);
    r.once('error', function (err) { cb(err) });
    r.pipe(through.obj(write, end));
    
    function write (row, enc, next) { cb(null, true) }
    function end () { cb(null, false) }
};

Ix.prototype.createReadStream = function (name, opts) {
    var self = this;
    if (!opts) opts = {};
    var nopts = wrap(opts || {}, {
        gt: function (x) {
            return [ name, defined(x, null), opts.gte ? null : undefined ];
        },
        lt: function (x) {
            return [ name, x, opts.lte ? undefined : null ];
        }
    });
    var decodeKey = decoder(self.ixdb.options.keyEncoding);
    return this.rdb.createReadStream(nopts)
        .pipe(through.obj(write))
    ;
    function write (row, enc, next) {
        var tr = this;
        var key = Buffer(row.key[row.key.length-1], 'hex');
        self.chdb.get(key, function (err, value) {
            if (err) return next(err);
            tr.push({
                key: decodeKey(key),
                value: value,
                index: row.key[1]
            });
            next();
        });
    }
};

Ix.prototype.clear = function (name, cb) {
    var self = this;
    var ops = [];
    if (!cb) cb = function () {};
    
    self.rdb.createReadStream({
        gt: [ name, null ],
        lt: [ name, undefined ]
    }).pipe(through.obj(write, end));
    
    function write (row, enc, next) {
        ops.push({ type: 'del', key: row.key });
        next();
    }
    function end () {
        self.rdb.db.batch(ops, cb);
    }
};

Ix.prototype._decode = function (x) {
    var d = changesdown.decode(x);
    var batch = d.type === 'batch' ? d.batch : [ d ];
    var options = this.ixdb.options;
    
    var codec = {
        decodeKey: decoder(options.keyEncoding),
        decodeValue: decoder(options.valueEncoding)
    };
    return batch.map(function (b) {
        return {
            type: b.type,
            key: unbuf(codec.decodeKey(b.key, options)),
            rawKey: b.key,
            value: codec.decodeValue(b.value, options),
            rawValue: b.value
        };
    });
    
    function id (x) { return x }
};

function unbuf (buf) {
    if (Buffer.isBuffer(buf)) return buf.toString('utf8');
    return buf;
}

function decoder (d) {
    if (d === 'utf8') {
        return function (x) { return x.toString('utf8') };
    }
    else if (d === 'json') {
        return function (x) { return JSON.parse(x) };
    }
    else if (d && typeof d.decode === 'function') {
        return d.decode;
    }
    else return function (x) { return x };
}
