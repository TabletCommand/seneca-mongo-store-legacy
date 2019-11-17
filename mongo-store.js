/* Copyright (c) 2010-2015 Richard Rodger, MIT License */
"use strict";

var _ = require("lodash");
var mongo = require("mongodb");
var MongoClient = mongo.MongoClient;
var ObjectID = mongo.ObjectID;

var name = "mongo-store-legacy";

/*
native$ = object => use object as query, no meta settings
native$ = array => use first elem as query, second elem as meta settings
*/

function idstr(obj) {
  return (obj && obj.toHexString) ? obj.toHexString() : "" + obj;
}

function makeid(hexstr) {
  if (_.isString(hexstr) && 24 === hexstr.length) {
    try {
      return ObjectID.createFromHexString(hexstr);
    } catch (e) {
      return hexstr;
    }
  }
  return hexstr;
}

function fixquery(qent, q) {
  var qq = {};
  if (!q.native$) {
    for (var qp in q) {
      if (!qp.match(/\$$/)) {
        qq[qp] = q[qp];
      }
    }
    if (qq.id) {
      qq._id = makeid(qq.id);
      delete qq.id;
    }
  } else {
    qq = _.isArray(q.native$) ? q.native$[0] : q.native$;
  }
  return qq;
}

function metaquery(qent, q) {
  var mq = {}

  if (!q.native$) {
    if (q.sort$) {
      for (var sf in q.sort$) {
        break;
      }
      var sd = q.sort$[sf] < 0 ? "descending" : "ascending";
      mq.sort = [
        [sf, sd]
      ];
    }

    if (q.limit$) {
      mq.limit = q.limit$;
    }

    if (q.skip$) {
      mq.skip = q.skip$;
    }

    if (q.fields$) {
      mq.fields = q.fields$;
    }
  } else {
    mq = _.isArray(q.native$) ? q.native$[1] : mq;
  }

  return mq;
}

module.exports = function exportsMongoStore(opts) {
  var seneca = this;
  var desc;

  var dbinst = null;
  var collmap = {};

  function error(args, err, cb) {
    if (err) {
      seneca.log.error("entity", err, {
        store: name
      });
      return cb(err);
    } else {
      return false;
    }
  }

  function configureold(spec, cb) {
    // defer connection
    // TODO: expose connection action
    if (!_.isUndefined(spec.connect) && !spec.connect) {
      return cb();
    }

    var conf = "string" == typeof(spec) ? null : spec;

    if (!conf) {
      conf = {};
      var urlM = /^mongo:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec);
      conf.name = urlM[7];
      conf.port = urlM[6];
      conf.server = urlM[4];
      conf.username = urlM[2];
      conf.password = urlM[3];

      conf.port = conf.port ? parseInt(conf.port, 10) : null;
    }

    conf.host = conf.host || conf.server;
    conf.username = conf.username || conf.user;
    conf.password = conf.password || conf.pass;

    var dbopts = seneca.util.deepextend({
      native_parser: false,
      auto_reconnect: true,
      w: 1
    }, conf.options);

    if (conf.replicaset) {
      var rservs = [];
      for (var i = 0; i < conf.replicaset.servers.length; i++) {
        var servconf = conf.replicaset.servers[i];
        rservs.push(new mongo.Server(servconf.host, servconf.port, dbopts));
      }
      var rset = new mongo.ReplSet(rservs);
      dbinst = new mongo.Db(conf.name, rset, dbopts);
    } else {
      dbinst = new mongo.Db(
        conf.name,
        new mongo.Server(
          conf.host || conf.server,
          conf.port || mongo.Connection.DEFAULT_PORT, {}
        ),
        dbopts
      );
    }

    dbinst.open(function(err) {
      if (err) {
        return seneca.die('open', err, conf);
      }

      if (conf.username) {
        dbinst.authenticate(conf.username, conf.password, function(err) {
          if (err) {
            seneca.log.error('init', 'db auth failed for ' + conf.username, dbopts);
            return cb(err);
          }

          seneca.log.debug('init', 'db open and authed for ' + conf.username, dbopts);
          return cb(null);
        })
      } else {
        seneca.log.debug('init', 'db open', dbopts);
        return cb(null);
      }
    })
  }

  function configure(conf, cb) {
    // defer connection
    // TODO: expose connection action
    if (!_.isUndefined(conf.connect) && !conf.connect) {
      return cb();
    }

    // Turn the hash into a mongo uri
    if (!conf.uri) {
      conf.uri = 'mongodb://';
      conf.uri += (conf.username) ? conf.username : '';
      conf.uri += (conf.password) ? ':' + conf.password + '@' : '';
      conf.uri += conf.host || conf.server;
      conf.uri += (conf.port) ? ':' + conf.port : '';
      conf.uri += '/' + (conf.db || conf.name);
    }

    // Extend the db options with some defaults
    // See http://mongodb.github.io/node-mongodb-native/2.0/reference/connecting/connection-settings/ for options
    var options = seneca.util.deepextend({
      db: {},
      server: {},
      replset: {},
      mongos: {}
    }, conf.options);

    // Connect using the URI
    return MongoClient.connect(conf.uri, options, function(err, db) {
      if (err) {
        return seneca.die('connect', err, conf);
      }

      // Set the instance to use throughout the plugin
      dbinst = db;
      seneca.log.debug('init', 'db open', conf);
      return cb(null);
    });
  }

  function getcoll(args, ent, cb) {
    var canon = ent.canon$({
      object: true
    });

    var collname = (canon.base ? canon.base + '_' : '') + canon.name;

    if (!collmap[collname]) {
      dbinst.collection(collname, function(err, coll) {
        if (!error(args, err, cb)) {
          collmap[collname] = coll;
          return cb(null, coll);
        }
      })
    } else {
      return cb(null, collmap[collname]);
    }
  }

  var store = {
    name: name,

    close: function(args, cb) {
      if (dbinst) {
        dbinst.close(cb);
      } else {
        return cb();
      }
    },

    save: function(args, cb) {
      var ent = args.ent;
      var update = !!ent.id;

      return getcoll(args, ent, function(err, coll) {
        if (!error(args, err, cb)) {
          var entp = {};

          var fields = ent.fields$();
          fields.forEach(function(field) {
            entp[field] = ent[field];
          });

          if (!update && void 0 != ent.id$) {
            entp._id = makeid(ent.id$);
          }

          if (update) {
            var q = {
              _id: makeid(ent.id)
            };
            delete entp.id;

            return coll.updateOne(q, {
              $set: entp
            }, {
              upsert: true
            }, function(err, update) {
              if (!error(args, err, cb)) {
                seneca.log.debug('save/update', ent, desc);
                return cb(null, ent);
              }
            });
          } else {
            return coll.insertOne(entp, {}, function(err, inserts) {
              if (!error(args, err, cb)) {
                ent.id = idstr(inserts.ops[0]._id)
                seneca.log.debug('save/insert', ent, desc);
                return cb(null, ent);
              }
            });
          }
        }
      });
    },


    load: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      return getcoll(args, qent, function(err, coll) {
        if (!error(args, err, cb)) {
          var mq = metaquery(qent, q);
          var qq = fixquery(qent, q);

          return coll.findOne(qq, mq, function(err, entp) {
            if (!error(args, err, cb)) {
              var fent = null;
              if (entp) {
                entp.id = idstr(entp._id);
                delete entp._id;
                fent = qent.make$(entp);
              }

              seneca.log.debug('load', q, fent, desc);
              return cb(null, fent);
            }
          });
        }
      });
    },

    list: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      return getcoll(args, qent, function(err, coll) {
        if (!error(args, err, cb)) {
          var mq = metaquery(qent, q);
          var qq = fixquery(qent, q);

          return coll.find(qq, mq, function(err, cur) {
            if (!error(args, err, cb)) {
              var list = [];

              cur.each(function(err, entp) {
                if (!error(args, err, cb)) {
                  if (entp) {
                    var fent = null;
                    if (entp) {
                      entp.id = idstr(entp._id);
                      delete entp._id;
                      fent = qent.make$(entp);
                    }
                    list.push(fent);
                  } else {
                    seneca.log.debug('list', q, list.length, list[0], desc);
                    return cb(null, list);
                  }
                }
              });
            }
          });
        }
      });
    },

    remove: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var all = q.all$; // default false
      var load = _.isUndefined(q.load$) ? true : q.load$; // default true

      return getcoll(args, qent, function(err, coll) {
        if (!error(args, err, cb)) {
          var qq = fixquery(qent, q);

          if (all) {
            return coll.deleteMany(qq, {}, function(err) {
              seneca.log.debug('remove/all', q, desc);
              return cb(err);
            });
          } else {
            var mq = metaquery(qent, q);
            return coll.findOne(qq, mq, function(err, entp) {
              if (!error(args, err, cb)) {
                if (entp) {
                  var deleteOneQuery = {
                    _id: entp._id
                  };
                  return coll.deleteOne(deleteOneQuery, {}, function(err) {
                    seneca.log.debug('remove/one', q, entp, desc);
                    var ent = load ? entp : null;
                    return cb(err, ent);
                  })
                } else {
                  return cb(null);
                }
              }
            });
          }
        }
      });
    },

    native: function(args, done) {
      return dbinst.collection('seneca', function(err, coll) {
        if (!error(args, err, done)) {
          return coll.findOne({}, {}, function(err, entp) {
            if (!error(args, err, done)) {
              return done(null, dbinst);
            } else {
              return done(err);
            }
          })
        } else {
          return done(err);
        }
      })
    }
  }

  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;

  seneca.add({
    init: store.name,
    tag: meta.tag
  }, function(args, done) {
    return configure(opts, function(err) {
      if (err) return seneca.die('store', err, {
        store: store.name,
        desc: desc
      });
      return done();
    });
  });

  return {
    name: store.name,
    tag: meta.tag
  };
};
