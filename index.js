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
    this.feed = opts.feed;
    this.names = {};
}

Ix.prototype.add = function (name, fn) {
    var self = this;
    var rdb = this._getName(name).result;
    var cdb = this._getName(name).change;
    var proc = chproc({ db: cdb, feed: this.feed, worker: worker });
    this.names[name] = { change: cdb, result: rdb };
    
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
                if (!indexes) next();
                if (typeof indexes !== 'object') {
                    return cb(new Error('object expected for the indexes'));
                }
                var batch = Object.keys(indexes).map(onmap);
                rdb.batch(batch, next);
                
                function onmap (key) {
                    return {
                        type: row.type,
                        key: [ name, key, indexes[key], row.key ],
                        value: '0'
                    };
                }
            });
        })();
    }
};

Ix.prototype.exists = function (xname, key, cb) {
    if (typeof xname === 'string') xname = xname.split('.');
    var name = xname[0], iname = xname[1];
    var ndb = this._getName(name);
    var opts = {
        gte: [ name, iname, key, null ],
        lte: [ name, iname, key, undefined ]
    };
    var r = ndb.result.createReadStream(opts);
    r.once('error', function (err) { cb(err) });
    r.pipe(through.obj(write, end));
    
    function write (row, enc, next) { cb(null, true) }
    function end () { cb(null, false) }
};

Ix.prototype.createReadStream = function (xname, opts) {
    var self = this;
    if (typeof xname === 'string') xname = xname.split('.');
    var name = xname[0], iname = xname[1];
    var ndb = this._getName(name);
    
    var nopts = wrap(opts || {}, {
        gt: function (x) {
            return [ name, iname, defined(x, null), undefined ];
        },
        lt: function (x) {
            return [ name, iname, x, null ];
        }
    });
    return ndb.result.createReadStream(nopts)
        .pipe(through.obj(write))
    ;
    function write (row, enc, next) {
        var tr = this;
        var key = row.key[row.key.length-1]
        self.chdb.get(key, function (err, value) {
            if (err) return next(err);
            tr.push({ key: key, value: value, index: row.key[2] });
            next();
        });
    }
};

Ix.prototype.clear = function (name, cb) {
    var rdb = this._getName(name).result;
    var cdb = this._getName(name).change;
    var ops = [];
    var pending = 2;
    
    rdb.createReadStream().pipe(through.obj(write, end));
    cdb.createReadStream().pipe(through.obj(write, end));
    
    function write (row, enc, next) {
        ops.push(row.key);
        next();
    }
    function end () {
        if (-- pending === 0) db.batch(ops, cb);
    }
};

Ix.prototype._getName = function (name) {
    if (!this.names[name]) {
        var rdb = sublevel(this.ixdb, 'r!' + name, { keyEncoding: bytewise });
        var cdb = sublevel(this.ixdb, 'c!' + name);
        this.names[name] = { result: rdb, change: cdb };
    }
    return this.names[name];
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
            value: codec.decodeValue(b.value, options)
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
