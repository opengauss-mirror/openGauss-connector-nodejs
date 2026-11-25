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

function testClientVersion(cb) {
  var client = new Client({})
  client.connect(
    assert.success(function () {
      helper.versionGTE(
        client,
        100000,
        assert.success(function (isGreater) {
          return client.end(
            assert.success(function () {
              if (!isGreater) {
                console.log(
                  'skip idle_in_transaction_session_timeout at client-level is only available in v10 and above'
                )
                return
              }
              cb()
            })
          )
        })
      )
    })
  )
}

function getIdleTransactionSessionTimeout(conf, cb) {
  var client = new Client(conf)
  client.connect(
    assert.success(function () {
      client.query(
        'SHOW idle_in_transaction_session_timeout',
        assert.success(function (res) {
          var timeout = res.rows[0].idle_in_transaction_session_timeout
          cb(timeout)
          client.end()
        })
      )
    })
  )
}

if (!helper.args.native) {
  // idle_in_transaction_session_timeout is not supported with the native client
  testClientVersion(function () {
    suite.test('No default idle_in_transaction_session_timeout ', function (done) {
      getConInfo()
      getIdleTransactionSessionTimeout({}, function (res) {
        assert.strictEqual(res, '0') // 0 = no timeout
        done()
      })
    })

    suite.test('idle_in_transaction_session_timeout integer is used', function (done) {
      var conf = getConInfo({
        idle_in_transaction_session_timeout: 3000,
      })
      getIdleTransactionSessionTimeout(conf, function (res) {
        assert.strictEqual(res, '3s')
        done()
      })
    })

    suite.test('idle_in_transaction_session_timeout float is used', function (done) {
      var conf = getConInfo({
        idle_in_transaction_session_timeout: 3000.7,
      })
      getIdleTransactionSessionTimeout(conf, function (res) {
        assert.strictEqual(res, '3s')
        done()
      })
    })

    suite.test('idle_in_transaction_session_timeout string is used', function (done) {
      var conf = getConInfo({
        idle_in_transaction_session_timeout: '3000',
      })
      getIdleTransactionSessionTimeout(conf, function (res) {
        assert.strictEqual(res, '3s')
        done()
      })
    })
  })
}
