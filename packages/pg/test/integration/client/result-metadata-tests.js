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
var pg = helper.pg

const pool = new pg.Pool()
new helper.Suite().test('should return insert metadata', function () {
  pool.connect(
    assert.calls(function (err, client, done) {
      assert(!err)

      helper.versionGTE(
        client,
        90000,
        assert.success(function (hasRowCount) {
          client.query(
            'CREATE TEMP TABLE zugzug(name varchar(10))',
            assert.calls(function (err, result) {
              assert(!err)
              assert.equal(result.oid, null)
              assert.equal(result.command, 'CREATE')

              var q = client.query(
                "INSERT INTO zugzug(name) VALUES('more work?')",
                assert.calls(function (err, result) {
                  assert(!err)
                  assert.equal(result.command, 'INSERT')
                  assert.equal(result.rowCount, 1)

                  client.query(
                    'SELECT * FROM zugzug',
                    assert.calls(function (err, result) {
                      assert(!err)
                      if (hasRowCount) assert.equal(result.rowCount, 1)
                      assert.equal(result.command, 'SELECT')
                      done()
                      process.nextTick(pool.end.bind(pool))
                    })
                  )
                })
              )
            })
          )
        })
      )
    })
  )
})
