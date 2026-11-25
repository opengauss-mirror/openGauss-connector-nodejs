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
var helper = require('../test-helper')
var exec = require('child_process').exec

helper.pg.defaults.poolIdleTimeout = 1000

const pool = new helper.pg.Pool()
pool.connect(function (err, client, done) {
  assert.ifError(err)
  client.once('error', function (err) {
    client.on('error', (err) => {})
    done(err)
  })
  client.query('SELECT pg_backend_pid()', function (err, result) {
    assert.ifError(err)
    var pid = result.rows[0].pg_backend_pid
    var psql = 'psql'
    if (helper.args.host) psql = psql + ' -h ' + helper.args.host
    if (helper.args.port) psql = psql + ' -p ' + helper.args.port
    if (helper.args.user) psql = psql + ' -U ' + helper.args.user
    exec(
      psql + ' -c "select pg_terminate_backend(' + pid + ')" template1',
      assert.calls(function (error, stdout, stderr) {
        assert.ifError(error)
      })
    )
  })
})
