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
var helper = require('./../test-helper')

const suite = new helper.Suite()

helper.testPoolSize = function (max) {
  suite.test(`test ${max} queries executed on a pool rapidly`, (cb) => {
    const pool = new helper.pg.Pool({ max: 10 })

    var sink = new helper.Sink(max, function () {
      pool.end(cb)
    })

    for (var i = 0; i < max; i++) {
      pool.connect(function (err, client, done) {
        assert(!err)
        client.query('SELECT * FROM NOW()')
        client.query('select generate_series(0, 25)', function (err, result) {
          assert.equal(result.rows.length, 26)
        })
        var query = client.query('SELECT * FROM NOW()', (err) => {
          assert(!err)
          sink.add()
          done()
        })
      })
    }
  })
}

module.exports = Object.assign({}, helper, { suite: suite })
