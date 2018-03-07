#!/bin/bash

set -eux

psql -h localhost -U postgres -c 'drop database dpeqtestdb;'
psql -h localhost -U postgres -c 'create database dpeqtestdb;'

dub -b debug
