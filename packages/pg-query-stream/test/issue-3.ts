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
