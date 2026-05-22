# openGauss-connector-nodejs

#### Introduction
A simple [openGauss ](https://opengauss.org)Node.js driver based on [node-postgres](https://github.com/brianc/node-postgres).


#### Installation

```bash
    npm install & npm run build
```
#### Instruction

- [Configure the client access authentication mode.](https://opengauss.org/zh/docs/2.0.1/docs/Developerguide/%E9%85%8D%E7%BD%AE%E5%AE%A2%E6%88%B7%E7%AB%AF%E6%8E%A5%E5%85%A5%E8%AE%A4%E8%AF%81.html)

- For details about driver implementation, see [node-postgres](https://github.com/brianc/node-postgres).

- For details about SHA256 & SM3 encryption, see [openGauss-connector-jdbc](https://gitee.com/opengauss/openGauss-connector-jdbc).

- For details about the usage, see [node-postgres wiki](https://node-postgres.com).

#### Precautions

- [SEMMNI error encountered during installation]

    During simple installation, the following error may occur:
  
   `"On systemwide basis, the maximum number of SEMMNI is not correct. the current SEMMNI value is: .... Please check it."`

  According to the installation script, the semaphore SEMMNI should be greater than 321.875.

  On CentOS 7, run the following command to modify SEMMNI:

  ```bash
  $ sudo vim /etc/sysctl.conf
  ```

  Add the following data to the end of the file:

  `kernel.sem = 250 32000 100 400`

  Run `shift+ double z` or `:wq` to save the file.

  Run the following command to apply the modification.

  ```bash
  $ /sbin/sysctl -p
  ```

- [To change the encryption mode, you need to update your password or create a user.]
  
    Otherwise, an error similar to "invalid username/password, login denied" will be displayed even if the username and password are correct.
    
  Run `select rolname,rolpassword from pg_authid;` to check the encryption mode of the current user password.
  
  [Account lockout]
  
  To unlock an openGauss account, log in to openGauss and run the following command:
  [database_name]#  alter user [username] account unlock;
  
- [Add an IPv4 rule to allow SHA256 to verify all external links.]

    Open `/opt/software/openGauss/data/single_node/pg_hba.conf`,
    and add the following data:
    add this:     `host    all        all         0.0.0.0/0           sha256 `
    For details, see [here](https://opengauss.org/zh/docs/1.0.0/docs/Quickstart/GUC%E5%8F%82%E6%95%B0%E8%AF%B4%E6%98%8E.html).

- [Errors that may occur during npminstall]
    
    If `Cannot find .....` is displayed, check whether the `Node.js` version is v16 or later, and
    whether the npm version is v7 or later. If not, install the latest `Node.js`.
  
    
#### Simple Test
Modify the server configuration in `packages/pg/test-1.js` and run the following commands:

```bash
cd packages/pg/
node test-1.js
```
