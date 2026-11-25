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

const Pool = require('../')

const expect = require('expect.js')
const net = require('net')

describe('releasing clients', () => {
  it('removes a client which cannot be queried', async () => {
    // make a pool w/ only 1 client
    const pool = new Pool({ max: 1 })
    expect(pool.totalCount).to.eql(0)
    const client = await pool.connect()
    expect(pool.totalCount).to.eql(1)
    expect(pool.idleCount).to.eql(0)
    // reach into the client and sever its connection
    client.connection.end()

    // wait for the client to error out
    const err = await new Promise((resolve) => client.once('error', resolve))
    expect(err).to.be.ok()
    expect(pool.totalCount).to.eql(1)
    expect(pool.idleCount).to.eql(0)

    // try to return it to the pool - this removes it because its broken
    client.release()
    expect(pool.totalCount).to.eql(0)
    expect(pool.idleCount).to.eql(0)

    // make sure pool still works
    const { rows } = await pool.query('SELECT NOW()')
    expect(rows).to.have.length(1)
    await pool.end()
  })

  it('removes a client which is ending', async () => {
    // make a pool w/ only 1 client
    const pool = new Pool({ max: 1 })
    expect(pool.totalCount).to.eql(0)
    const client = await pool.connect()
    expect(pool.totalCount).to.eql(1)
    expect(pool.idleCount).to.eql(0)
    // end the client gracefully (but you shouldn't do this with pooled clients)
    client.end()

    // try to return it to the pool
    client.release()
    expect(pool.totalCount).to.eql(0)
    expect(pool.idleCount).to.eql(0)

    // make sure pool still works
    const { rows } = await pool.query('SELECT NOW()')
    expect(rows).to.have.length(1)
    await pool.end()
  })
})
