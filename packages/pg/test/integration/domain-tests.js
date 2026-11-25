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
var async = require('async')

var helper = require('./test-helper')
var Query = helper.pg.Query
var suite = new helper.Suite()

const Pool = helper.pg.Pool

suite.test('no domain', function (cb) {
  assert(!process.domain)
  const pool = new Pool()
  pool.connect(
    assert.success(function (client, done) {
      assert(!process.domain)
      done()
      pool.end(cb)
    })
  )
})

suite.test('with domain', function (cb) {
  assert(!process.domain)
  const pool = new Pool()
  var domain = require('domain').create()
  domain.run(function () {
    var startingDomain = process.domain
    assert(startingDomain)
    pool.connect(
      assert.success(function (client, done) {
        assert(process.domain, 'no domain exists in connect callback')
        assert.equal(startingDomain, process.domain, 'domain was lost when checking out a client')
        var query = client.query(
          'SELECT NOW()',
          assert.success(function () {
            assert(process.domain, 'no domain exists in query callback')
            assert.equal(startingDomain, process.domain, 'domain was lost when checking out a client')
            done(true)
            process.domain.exit()
            pool.end(cb)
          })
        )
      })
    )
  })
})

suite.test('error on domain', function (cb) {
  var domain = require('domain').create()
  const pool = new Pool()
  domain.on('error', function () {
    pool.end(cb)
  })
  domain.run(function () {
    pool.connect(
      assert.success(function (client, done) {
        client.query(new Query('SELECT SLDKJFLSKDJF'))
        client.on('drain', done)
      })
    )
  })
})
