/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

'use strict'
var helper = require('./test-helper')
var Query = helper.pg.Query

// before running this test make sure you run the script create-test-tables
test('simple query interface', function () {
  var client = helper.client()

  var query = client.query(new Query('select name from person order by name collate "C"'))

  client.on('drain', client.end.bind(client))

  var rows = []
  query.on('row', function (row, result) {
    assert.ok(result)
    rows.push(row['name'])
  })
  query.once('row', function (row) {
    test('Can iterate through columns', function () {
      var columnCount = 0
      for (var column in row) {
        columnCount++
      }
      if ('length' in row) {
        assert.lengthIs(
          row,
          columnCount,
          'Iterating through the columns gives a different length from calling .length.'
        )
      }
    })
  })

  assert.emits(query, 'end', function () {
    test('returned right number of rows', function () {
      assert.lengthIs(rows, 26)
    })
    test('row ordering', function () {
      assert.equal(rows[0], 'Aaron')
      assert.equal(rows[25], 'Zanzabar')
    })
  })
})

test('prepared statements do not mutate params', function () {
  var client = helper.client()

  var params = [1]

  var query = client.query(new Query('select name from person where $1 = 1 order by name collate "C"', params))

  assert.deepEqual(params, [1])

  client.on('drain', client.end.bind(client))

  const rows = []
  query.on('row', function (row, result) {
    assert.ok(result)
    rows.push(row)
  })

  query.on('end', function (result) {
    assert.lengthIs(rows, 26, 'result returned wrong number of rows')
    assert.lengthIs(rows, result.rowCount)
    assert.equal(rows[0].name, 'Aaron')
    assert.equal(rows[25].name, 'Zanzabar')
  })
})

test('multiple simple queries', function () {
  var client = helper.client()
  client.query({ text: "create temp table bang(id serial, name varchar(5));insert into bang(name) VALUES('boom');" })
  client.query("insert into bang(name) VALUES ('yes');")
  var query = client.query(new Query('select name from bang'))
  assert.emits(query, 'row', function (row) {
    assert.equal(row['name'], 'boom')
    assert.emits(query, 'row', function (row) {
      assert.equal(row['name'], 'yes')
    })
  })
  client.on('drain', client.end.bind(client))
})

test('multiple select statements', function () {
  var client = helper.client()
  client.query(
    'create temp table boom(age integer); insert into boom(age) values(1); insert into boom(age) values(2); insert into boom(age) values(3)'
  )
  client.query({ text: "create temp table bang(name varchar(5)); insert into bang(name) values('zoom');" })
  var result = client.query(new Query({ text: 'select age from boom where age < 2; select name from bang' }))
  assert.emits(result, 'row', function (row) {
    assert.strictEqual(row['age'], 1)
    assert.emits(result, 'row', function (row) {
      assert.strictEqual(row['name'], 'zoom')
    })
  })
  client.on('drain', client.end.bind(client))
})
