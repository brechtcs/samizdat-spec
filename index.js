var pull = require('pull-stream')
var ts = require('samizdat-ts')

module.exports.basic = function (name, opts) {
    var test = opts.tape
    var db = opts.db

    test(name + ': create and read new entries', function (t) {
      t.plan(11)

      db.create('dit', 'deze', function (err, first) {
        t.notOk(err, 'create first entry')
        t.ok(ts.validate(first.key), 'created entry key is valid')

        db.create('dat', 'die', function (err, second) {
          t.notOk(err, 'create second entry')

          db.read(first.key, function (err, value) {
            t.notOk(err, 'read first entry')
            t.equal(value, 'deze', 'first entry value matches input')
          })

          db.read(second.key, function (err, value) {
            t.notOk(err, 'read second entry')
            t.equal(value, 'die', 'second entry value matches input')
          })

          db.docs(function (err, entries) {
            t.notOk(err, 'check all entered docs')
            t.equal(entries.length, 2, 'docs call returns two entries')
            t.ok(entries.includes('dit'), 'first entry is present')
            t.ok(entries.includes('dat'), 'second entry is present')
          })
        })
      })
    })

    test(name + ': create and update entry, read both versions, and check history', function (t) {
      t.plan(10)

      db.create('qds74e412-000000000-entry', 'stuff', function (err) {
        t.ok(err && err.invalidId, 'new entry id cannot be valid database key')
      })

      db.create('some', 'stuff', function (err, data) {
        setTimeout(function () {
          db.update(data.key, 'things', function (err, data) {
            t.notOk(err, 'update entry')
            t.ok(ts.validate(data.key), 'updated entry key is valid')
            t.ok(ts.validate(data.prev), 'previous entry key is valid')

            db.read(data.prev, function (err, value) {
              t.notOk(err, 'read older version of updated entry')
              t.equal(value, 'stuff', 'requested version returns correctly')
            })

            db.history('some', function (err, versions) {
              t.notOk(err, 'check history of entry')
              t.equal(versions.length, 2, 'history call returns two versions')
              t.equal(versions[0], data.prev, 'first version key is correct')
              t.equal(versions[1], data.key, 'second version key is correct')
            })
          })
        }, 10)
      })
    })
}

module.exports.stream = function (name, opts) {
    var test = opts.tape
    var db = opts.db

    test(name + ': stream raw entries in and out', function (t) {
      t.plan(5)

      var entries = [
        {key: '1k178m1unww-00000000000-arf', value: 'barf'},
        {key: '1k178m1unx3-00000000000-yarf', value: 'gnarf'}
      ]

      pull(
        pull.values(entries),
        db.sink(function (err) {
          t.notOk(err, 'Stream raw entries into database')

          pull(
            db.source(),
            pull.collect(function (err, result) {
              t.notOk(err, 'Stream raw entries out of database')
              t.equal(result.length, entries.length, 'Create right number of entries')
              t.deepEqual(result[0], entries[0], 'First entry is correct')
              t.deepEqual(result[1], entries[1], 'Second entry is correct')
            })
          )
        })
      )
    })
}
