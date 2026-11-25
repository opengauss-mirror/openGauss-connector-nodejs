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

const { Promise } = require('bluebird')
const { Pool, Client } = require('pg')
// const isMd5 = false
// const client = true

const ogConfig = {
  user: 'tyt',
  database: 'postgres',
  password: 'Tyt123456',
  host: '192.168.43.201',
  port: 5432,
}
const pool = new Pool(ogConfig)
const client = new Client(ogConfig)

var now = Date.now()
client.connect()

client.query('create table t3(a int, b int);', (err, res) => {
  console.log(err, res)
})
client.query('BEGIN;', (err, res) => {
  console.log(err, res)
})
client.query('insert into t3 values(1, 2);', (err, res) => {
  console.log(err, res)
})
client.query('create table t2(c int, d int);', (err, res) => {
  console.log(err, res)
})
client.query('insert into t2 values(3, 4);', (err, res) => {
  console.log(err, res)
})
client.query('COMMIT;', (err, res) => {
  console.log(err, res)
})
client.query('SELECT * FROM t3;', (err, res) => {
  console.log(err, res)
})
client.query('BEGIN;', (err, res) => {
  console.log(err, res)
})
client.query('insert into t3 values(3, 4);', (err, res) => {
  console.log(err, res)
})
client.query('SELECT * FROM t3', (err, res) => {
  console.log(err, res)
})
client.query('ROLLBACK;', (err, res) => {
  console.log(err, res)
})
client.query('SELECT * FROM t3', (err, res) => {
  console.log(err, res)
})
client.query('BEGIN;', (err, res) => {
  console.log(err, res)
})
client.query('SAVEPOINT myTest', (err, res) => {
  console.log(err, res)
})
client.query('DROP TABLE  t3;', (err, res) => {
  console.log(err, res)
})
client.query('DROP TABLE  t2;', (err, res) => {
  console.log(err, res)
})
client.query('SELECT * FROM t2;', (err, res) => {
  console.log(err, res)
})
client.query('ROLLBACK TO myTest;', (err, res) => {
  console.log(err, res)
})
client.query('SELECT * FROM t2;', (err, res) => {
  console.log(err, res)
})
client.query('BEGIN;', (err, res) => {
  console.log(err, res)
})
client.query('DROP TABLE  t3;', (err, res) => {
  console.log(err, res)
})
client.query('DROP TABLE  t2;', (err, res) => {
  console.log(err, res)
})
client.query('COMMIT;', (err, res) => {
  console.log(err, res)
  console.log('\n\x1b[32mAll queries & client end process complete in \x1b[1m' + (Date.now() - now) + ' ms')
  client.end()
})
