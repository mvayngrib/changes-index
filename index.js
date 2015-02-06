var chproc = require('level-change-processor');
var sublevel = require('subleveldown');
var isarray = require('isarray');
var inherits = require('inherits');
var EventEmitter = require('events').EventEmitter;
var changesdown = require('changesdown');
var defined = require('defined');
var through = require('through2');
var readonly = require('read-only-stream');
var bytewise = require('bytewise');
var wrap = require('level-option-wrap');
var has = require('has');

module.exports = Ix;
inherits(Ix, EventEmitter);

function Ix (opts) {
    var self = this;
    if (!(this instanceof Ix)) return new Ix(opts);
    EventEmitter.call(this);
    if (!opts) opts = {};
    this.ixdb = opts.ixdb;
    this.chdb = opts.chdb;
    
    this.rdb = sublevel(this.ixdb, 'r', {
        keyEncoding: bytewise,
        valueEncoding: 'json'
    });
    this.cdb = sublevel(this.ixdb, 'c');
    
    this.feed = opts.feed;
    this.names = {};
    this._latest = -1;
    
    this.cdb.get('latest', function (err, value) {
        if (value === undefined) value = '0';
        self._latest = parseInt(value);
        self.emit('latest', self._latest);
    });
}

Ix.prototype.add = function (fn) {
    var self = this;
    var proc = chproc({
        db: self.cdb,
        feed: self.feed,
        worker: function (ch, cb) {
            self._worker(fn, ch, cb);
        }
    });
    proc.on('error', function (err) {
        self.emit('error', err);
    });
};

Ix.prototype._worker = function (fn, ch, cb) {
    var self = this;
    var bkeys = {};
    var rows = self._decode(ch.value);
    next();
    
    function next (err) {
        if (err) return cb(err);
        if (rows.length === 0) {
            self._latest = ch.change;
            self.emit('latest', ch.change);
            self.emit('change', ch);
            return cb();
        }
        var row = rows.shift();
        
        if (has(bkeys, row.key)) {
            var keys = bkeys[row.key];
            if (row.type === 'del') {
                bkeys[row.key] = null;
            }
            if (keys === null) {
                onget({ type: 'NotFoundError' });
            }
            else onget(null, keys);
        }
        else self.rdb.get([ null, row.rawKey ], onget);
        
        function onget (err, keys) {
            row.exists = !(err && err.type === 'NotFoundError');
            row.prev = keys;
            row.change = ch.change;
            
            bkeys[row.key] = keys;
            
            fn(row, function (err, indexes) {
                if (err) return cb(err);
                if (!indexes) indexes = {};
                
                if (typeof indexes !== 'object') {
                    cb(new Error('object expected for the indexes'));
                }
                else onrow(row, indexes, keys)
            });
        }
    }
    function onrow (row, indexes, prev) {
        var pkeys = Object.keys(prev || {});
        var delbatch = pkeys.map(function (key) {
            return {
                type: 'del',
                key: [ key, prev[key], row.rawKey ]
            };
        });
        var batch = row.type === 'put'
            ? Object.keys(indexes).map(map)
            : []
        ;
        var keys = batch.map(function (b) {
            return b.key.slice(0, 2);
        });
        
        if (batch.length > 0) {
            batch.push({
                type: row.type,
                key: [ null, row.rawKey ],
                value: indexes
            });
            self.rdb.batch(delbatch.concat(batch), next);
        }
        else if (delbatch.length > 0) {
            self.rdb.batch(delbatch, next);
        }
        else next()
        
        function map (key) {
            return {
                type: row.type,
                key: [ key, indexes[key], row.rawKey ],
                value: 0
            };
        }
    }
};

Ix.prototype.createReadStream = function (name, opts) {
    var self = this;
    if (!opts) opts = {};
    if (opts.eq) {
        opts.lte = opts.eq;
        opts.gte = opts.eq;
    }
    
    var nopts = wrap(opts || {}, {
        gt: function (x) {
            return [
                name, defined(x, null),
                opts.gte !== undefined ? null : undefined
            ];
        },
        lt: function (x) {
            return [
                name, x,
                opts.lte !== undefined ? undefined : null
            ];
        }
    });
    var decodeKey = decoder(self.ixdb.options.keyEncoding);
    var r = through.obj(write);
    
    var feedch = self.feed.change;
    if (self._latest < feedch) {
        self.on('latest', function f (ch) {
            if (ch >= feedch) {
                self.removeListener('latest', f);
                self.rdb.createReadStream(nopts).pipe(r);
            }
        });
    }
    else {
        self.rdb.createReadStream(nopts).pipe(r);
    }
    return readonly(r);
    
    function write (row, enc, next) {
        var tr = this;
        var key = row.key[row.key.length-1];
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
