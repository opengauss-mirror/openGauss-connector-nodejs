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
var domain = require('domain')
var helper = require('./../test-helper')
var Client = require('./../../lib/native')
const suite = new helper.Suite()

suite.test('fires callback with results', function (done) {
  var client = new Client(helper.config)
  client.connect()
  client.query(
    'SELECT 1 as num',
    assert.calls(function (err, result) {
      assert(!err)
      assert.equal(result.rows[0].num, 1)
      assert.strictEqual(result.rowCount, 1)
      client.query(
        'SELECT * FROM person WHERE name = $1',
        ['Brian'],
        assert.calls(function (err, result) {
          assert(!err)
          assert.equal(result.rows[0].name, 'Brian')
          client.end(done)
        })
      )
    })
  )
})

suite.test('preserves domain', function (done) {
  var dom = domain.create()

  dom.run(function () {
    var client = new Client(helper.config)
    assert.ok(dom === require('domain').active, 'domain is active')
    client.connect()
    client.query('select 1', function () {
      assert.ok(dom === require('domain').active, 'domain is still active')
      client.end(done)
    })
  })
})
