/* Copyright (c) 2014 Richard Rodger, MIT License */

var assert = require("chai").assert;

var async = require("async");
var _ = require("lodash");
var gex = require("gex");

var bartemplate = {
  name$: 'bar',
  base$: 'moon',
  zone$: 'zen',

  str: 'aaa',
  int: 11,
  dec: 33.33,
  bol: false,
  wen: new Date(2020, 1, 1),
  arr: [2, 3],
  obj: {
    a: 1,
    b: [2],
    c: {
      d: 3
    }
  }
};

function barverify(bar) {
  assert.equal('aaa', bar.str);
  assert.equal(11, bar.int);
  assert.equal(33.33, bar.dec);
  assert.equal(false, bar.bol);
  assert.equal(new Date(2020, 1, 1).toISOString(), _.isDate(bar.wen) ? bar.wen.toISOString() : bar.wen);

  assert.equal('' + [2, 3], '' + bar.arr);
  assert.deepEqual({
    a: 1,
    b: [2],
    c: {
      d: 3
    }
  }, bar.obj);
}

var scratch = {};

function verify(cb, testsFunction) {
  return function(error, out) {
    if (error) {
      return cb(error);
    }

    testsFunction(out);
    return cb();
  };
}


function basictest(si, settings, done) {
  if (_.isFunction(settings)) {
    done = settings;
    settings = {};
  }

  si.ready(function() {
    console.log('BASIC');
    assert.isNotNull(si);

    var must_merge = null == settings.must_merge ? false : settings.must_merge;

    // TODO: test load$(string), remove$(string)

    /* Set up a data set for testing the store.
     * //foo contains [{p1:'v1',p2:'v2'},{p2:'v2'}]
     * zen/moon/bar contains [{..bartemplate..}]
     */
    async.series({
        load0: function load0(cb) {
          console.log('load0');
          var foo0 = si.make('foo');
          return foo0.load$('does-not-exist-at-all-at-all', verify(cb, function(out) {
            assert.isNull(out);
          }));
        },

        save1: function save1(cb) {
          console.log('save1');
          var foo1 = si.make({
            name$: 'foo'
          }); ///si.make('foo')
          foo1.p1 = 'v1';
          foo1.p3 = 'v3';
          return foo1.save$(verify(cb, function(foo1) {
            assert.isNotNull(foo1.id);
            assert.equal('v1', foo1.p1);
            assert.equal('v3', foo1.p3);
            scratch.foo1 = foo1;
          }));
        },

        load1: function load1(cb) {
          console.log('load1', scratch);
          scratch.foo1.load$(scratch.foo1.id, verify(cb, function(foo1) {
            console.log("foo1", foo1);
            assert.isNotNull(foo1.id);
            assert.equal('v1', foo1.p1);
            scratch.foo1 = foo1;
          }));
        },

        save2: function save2(cb) {
          console.log('save2');

          scratch.foo1.p1 = 'v1x';
          scratch.foo1.p2 = 'v2';

          // test merge behaviour
          delete scratch.foo1.p3;

          return scratch.foo1.save$(verify(cb, function(foo1) {
            assert.isNotNull(foo1.id);
            assert.equal('v1x', foo1.p1);
            assert.equal('v2', foo1.p2);

            if (must_merge) {
              assert.equal('v3', foo1.p3);
            }

            scratch.foo1 = foo1;
          }));
        },

        load2: function load2(cb) {
          console.log('load2');

          return scratch.foo1.load$(scratch.foo1.id, verify(cb, function(foo1) {
            assert.isNotNull(foo1.id);
            assert.equal('v1x', foo1.p1);
            assert.equal('v2', foo1.p2);
            scratch.foo1 = foo1;
          }));
        },

        save3: function save3(cb) {
          console.log('save3');

          scratch.bar = si.make(bartemplate);
          var mark = scratch.bar.mark = Math.random();

          return scratch.bar.save$(verify(cb, function(bar) {
            assert.isNotNull(bar.id);
            barverify(bar);
            assert.equal(mark, bar.mark);
            scratch.bar = bar;
          }));
        },

        save4: function save4(cb) {
          console.log('save4');

          scratch.foo2 = si.make({
            name$: 'foo'
          });
          scratch.foo2.p2 = 'v2';

          scratch.foo2.save$(verify(cb, function(foo2) {
            assert.isNotNull(foo2.id);
            assert.equal('v2', foo2.p2);
            scratch.foo2 = foo2;
          }));
        },

        save5: function save5(cb) {
          console.log('save5');

          scratch.foo2 = si.make({
            name$: 'foo'
          });
          scratch.foo2.id$ = 'zxy';

          return scratch.foo2.save$(verify(cb, function(foo2) {
            assert.isNotNull(foo2.id);
            assert.equal('zxy', foo2.id);
            scratch.foo2 = foo2;
          }));
        },

        query1: function query1(cb) {
          console.log('query1');

          scratch.barq = si.make('zen', 'moon', 'bar');
          return scratch.barq.list$({}, verify(cb, function(res) {
            assert.ok(1 <= res.length);
            barverify(res[0]);
          }));
        },

        query2: function query2(cb) {
          console.log('query2');
          return scratch.foo1.list$({}, verify(cb, function(res) {
            assert.ok(2 <= res.length);
          }));
        },

        query3: function query3(cb) {
          console.log('query3');
          return scratch.barq.list$({
            id: scratch.bar.id
          }, verify(cb, function(res) {
            assert.equal(1, res.length);
            barverify(res[0]);
          }));
        },

        query4: function query4(cb) {
          console.log('query4');
          return scratch.bar.list$({
            mark: scratch.bar.mark
          }, verify(cb, function(res) {
            assert.equal(1, res.length);
            barverify(res[0]);
          }));
        },

        query5: function query5(cb) {
          console.log('query5');
          return scratch.foo1.list$({
            p2: 'v2'
          }, verify(cb, function(res) {
            assert.ok(2 <= res.length);
          }));
        },

        query6: function query6(cb) {
          console.log('query6');
          return scratch.foo1.list$({
            p2: 'v2',
            p1: 'v1x'
          }, verify(cb, function(res) {
            assert.ok(1 <= res.length);
            res.forEach(function(foo) {
              assert.equal('v2', foo.p2);
              assert.equal('v1x', foo.p1);
            });
          }));
        },

        remove1: function remove1(cb) {
          console.log('remove1');
          var foo = si.make({
            name$: 'foo'
          });
          return foo.remove$({
            all$: true
          }, function(err, res) {
            assert.isNull(err);
            return foo.list$({}, verify(cb, function(res) {
              assert.equal(0, res.length);
            }));
          });
        },

        remove2: function remove2(cb) {
          console.log('remove2');
          scratch.bar.remove$({
            mark: scratch.bar.mark
          }, function(err, res) {
            assert.isNull(err);
            return scratch.bar.list$({
              mark: scratch.bar.mark
            }, verify(cb, function(res) {
              assert.equal(0, res.length);
            }));
          });
        },

      },
      function callback(err, out) {
        err = err || null;
        if (err) {
          console.dir(err);
        }
        si.__testcount++;
        assert.isNull(err);
        done && done();
      });
  });
}

function sqltest(si, done) {
  return si.ready(function() {
    assert.isNotNull(si);

    var Product = si.make('product');
    var products = [];

    async.series({
        setup: function(cb) {
          products.push(Product.make$({
            name: 'apple',
            price: 100
          }));
          products.push(Product.make$({
            name: 'pear',
            price: 200
          }));

          var i = 0;

          function saveproduct() {
            return function(cb) {
              products[i].save$(cb);
              i++;
            }
          }

          async.series([
            saveproduct(),
            saveproduct(),
          ], cb);
        },


        query_string: function(cb) {
          Product.list$("SELECT * FROM product ORDER BY price", function(err, list) {
            var s = _.map(list, function(p) {
              return p.toString()
            }).toString()
            assert.ok(
              gex("//product:{id=*;name=apple;price=100},//product:{id=*;name=pear;price=200}").on(s))
            cb();
          })
        },

        query_params: function(cb) {
          Product.list$(["SELECT * FROM product WHERE price >= ? AND price <= ?", 0, 1000], function(err, list) {
            var s = _.map(list, function(p) {
              return p.toString()
            }).toString()
            assert.ok(
              gex("//product:{id=*;name=apple;price=100},//product:{id=*;name=pear;price=200}").on(s))
            cb();
          });
        },

        teardown: function(cb) {
          products.forEach(function(p) {
            p.remove$();
          })
          return cb();
        }
      },
      function(err, out) {
        if (err) {
          console.dir(err);
        }
        si.__testcount++;
        assert.isNull(err);
        done && done();
      }
    )
  });
}

function closetest(si, testcount, done) {
  var RETRY_LIMIT = 10;
  var retryCnt = 0;

  function retry() {
    //console.log(testcount+' '+si.__testcount)
    if (testcount <= si.__testcount || retryCnt > RETRY_LIMIT) {
      console.log('CLOSE');
      si.close();
      done && done();
    } else {
      retryCnt++
      setTimeout(retry, 500);
    }
  }
  retry();
}

exports.closetest = closetest;
exports.verify = verify;
exports.basictest = basictest;
exports.sqltest = sqltest;
