# DPEQ - native PSQL extended protocol implementation for D programming language

[![Build Status](https://travis-ci.org/Boris-Barboris/dpeq.svg?branch=master)](https://travis-ci.org/Boris-Barboris/dpeq)

**dpeq** is a library that implements a subset of PostgreSQL wire protocol and focuses 
on extended query (EQ) protocol subset. **dpeq** defines classes
to hold the required state and utility functions, that send and receive protocol
messages in sensible manner.

**dpeq** is aimed on library developers that write database middleware for
Postgres or CockroachDB.

Here is a list of good links to get yourself familiar with EQ protocol, wich may
help you to understand the nature of the messages being passed:   
https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf   
https://www.postgresql.org/docs/9.5/static/protocol.html   
https://www.postgresql.org/docs/9.5/static/protocol-flow.html   
https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html   

Many thanks to authors of https://github.com/pszturmaj/ddb and
https://github.com/teamhackback/hb-ddb, wich gave this library inspiration.

To see examples, see ./tests/source/main.d