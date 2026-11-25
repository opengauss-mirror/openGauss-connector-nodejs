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

import pg from 'pg'
import QueryStream from '../src'

describe('end semantics race condition', function () {
  before(function (done) {
    const client = new pg.Client()
    client.connect()
    client.on('drain', client.end.bind(client))
    client.on('end', done)
    client.query('create table IF NOT EXISTS p(id serial primary key)')
    client.query('create table IF NOT EXISTS c(id int primary key references p)')
  })
  it('works', function (done) {
    const client1 = new pg.Client()
    client1.connect()
    const client2 = new pg.Client()
    client2.connect()

    const qr = new QueryStream('INSERT INTO p DEFAULT VALUES RETURNING id')
    client1.query(qr)
    let id = null
    qr.on('data', function (row) {
      id = row.id
    })
    qr.on('end', function () {
      client2.query('INSERT INTO c(id) VALUES ($1)', [id], function (err, rows) {
        client1.end()
        client2.end()
        done(err)
      })
    })
  })
})
