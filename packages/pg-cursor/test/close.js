/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const assert = require('assert')
const Cursor = require('../')
const pg = require('pg')

const text = 'SELECT generate_series as num FROM generate_series(0, 50)'
describe('close', function () {
  beforeEach(function (done) {
    const client = (this.client = new pg.Client())
    client.connect(done)
  })

  this.afterEach(function (done) {
    this.client.end(done)
  })

  it('can close a finished cursor without a callback', function (done) {
    const cursor = new Cursor(text)
    this.client.query(cursor)
    this.client.query('SELECT NOW()', done)
    cursor.read(100, function (err) {
      assert.ifError(err)
      cursor.close()
    })
  })

  it('closes cursor early', function (done) {
    const cursor = new Cursor(text)
    this.client.query(cursor)
    this.client.query('SELECT NOW()', done)
    cursor.read(25, function (err) {
      assert.ifError(err)
      cursor.close()
    })
  })

  it('works with callback style', function (done) {
    const cursor = new Cursor(text)
    const client = this.client
    client.query(cursor)
    cursor.read(25, function (err, rows) {
      assert.ifError(err)
      assert.strictEqual(rows.length, 25)
      cursor.close(function (err) {
        assert.ifError(err)
        client.query('SELECT NOW()', done)
      })
    })
  })

  it('is a no-op to "close" the cursor before submitting it', function (done) {
    const cursor = new Cursor(text)
    cursor.close(done)
  })
})
