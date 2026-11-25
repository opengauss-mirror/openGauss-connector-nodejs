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
var Client = helper.Client

var suite = new helper.Suite()

var conInfo = helper.config

function getConInfo(override) {
  return Object.assign({}, conInfo, override)
}

function getStatementTimeout(conf, cb) {
  var client = new Client(conf)
  client.connect(
    assert.success(function () {
      client.query(
        'SHOW statement_timeout',
        assert.success(function (res) {
          var statementTimeout = res.rows[0].statement_timeout
          cb(statementTimeout)
          client.end()
        })
      )
    })
  )
}

if (!helper.args.native) {
  // statement_timeout is not supported with the native client
  suite.test('No default statement_timeout ', function (done) {
    getConInfo()
    getStatementTimeout({}, function (res) {
      assert.strictEqual(res, '0') // 0 = no timeout
      done()
    })
  })

  suite.test('statement_timeout integer is used', function (done) {
    var conf = getConInfo({
      statement_timeout: 3000,
    })
    getStatementTimeout(conf, function (res) {
      assert.strictEqual(res, '3s')
      done()
    })
  })

  suite.test('statement_timeout float is used', function (done) {
    var conf = getConInfo({
      statement_timeout: 3000.7,
    })
    getStatementTimeout(conf, function (res) {
      assert.strictEqual(res, '3s')
      done()
    })
  })

  suite.test('statement_timeout string is used', function (done) {
    var conf = getConInfo({
      statement_timeout: '3000',
    })
    getStatementTimeout(conf, function (res) {
      assert.strictEqual(res, '3s')
      done()
    })
  })

  suite.test('statement_timeout actually cancels long running queries', function (done) {
    var conf = getConInfo({
      statement_timeout: '10', // 10ms to keep tests running fast
    })
    var client = new Client(conf)
    client.connect(
      assert.success(function () {
        client.query('SELECT pg_sleep( 1 )', function (error) {
          client.end()
          assert.strictEqual(error.code, '57014') // query_cancelled
          done()
        })
      })
    )
  })
}
