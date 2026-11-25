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

function testTypeParser(client, expectedResult, done) {
  var boolValue = true
  client.query('CREATE TEMP TABLE parserOverrideTest(id bool)')
  client.query('INSERT INTO parserOverrideTest(id) VALUES ($1)', [boolValue])
  client.query(
    'SELECT * FROM parserOverrideTest',
    assert.success(function (result) {
      assert.equal(result.rows[0].id, expectedResult)
      done()
    })
  )
}

const pool = new helper.pg.Pool(helper.config)
pool.connect(
  assert.success(function (client1, done1) {
    pool.connect(
      assert.success(function (client2, done2) {
        var boolTypeOID = 16
        client1.setTypeParser(boolTypeOID, function () {
          return 'first client'
        })
        client2.setTypeParser(boolTypeOID, function () {
          return 'second client'
        })

        client1.setTypeParser(boolTypeOID, 'binary', function () {
          return 'first client binary'
        })
        client2.setTypeParser(boolTypeOID, 'binary', function () {
          return 'second client binary'
        })

        testTypeParser(client1, 'first client', () => {
          done1()
          testTypeParser(client2, 'second client', () => done2(), pool.end())
        })
      })
    )
  })
)
