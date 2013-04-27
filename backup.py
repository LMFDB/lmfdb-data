#!/usr/bin/env python
# -*- coding: utf8 -*-

# global list of backup types
BACKUPS = ['knowl', 'user']

import os

def timestamp():
    'helper function that creates an ISO timestamp'
    from datetime import datetime
    return datetime.strftime(datetime.utcnow(), '%Y%m%d-%H%M')


def execute(command):
    'this little helper actually executes the given tokenized command and executes it. shell is bypassed!'
    from subprocess import Popen, PIPE, STDOUT
    import sys
    command = map(str, command)  # in case an int slipped in or smth like that
    process = Popen(command,
                    shell=False,
                    stdout=PIPE,
                    stderr=STDOUT)

    # Poll process for new output until finished
    while True:
        nextline = process.stdout.readline()
        if nextline == '' and process.poll() is not None:
            break
        sys.stdout.write(nextline)
        sys.stdout.flush()

    output = process.communicate()[0]
    exitCode = process.returncode
    if (exitCode == 0):
        return output
    else:
        print 'ERROR:', output
        print 'Exit =', exitCode
        print 'CMD:', ' '.join(command)


class LMFBDBackup(object):
    'this class contains functions to backup LMFDB'
    def __init__(self):
        self.args = self._parse_args()
        # print "ARGS:", self.args

        if not os.path.exists(self.args.dir):
            print "creating", self.args.dir
            os.makedirs(self.args.dir)

        for w in self.args.what:
            getattr(self, 'backup_%s' % w)()

    def export(self, db, col, outfn):
        '''helper function, that creates the list of tokens for running the mongoexport utility'
        and then exports the given collection into the outfn file'''
        cmd = ['mongoexport',
               '-host', self.args.dbhost, '--port', self.args.dbport,
               '-d', db, '-c', col,
               '-o', outfn]
        return execute(cmd)

    def backup_knowl(self):
        print "backup of all knowls"
        outfn = os.path.join(self.args.dir, "knowls-%s.json" % timestamp())
        print self.export('knowledge', 'knowls', outfn)

    def backup_user(self):
        print "backup of all users"
        outfn = os.path.join(self.args.dir, "users-%s.json" % timestamp())
        print self.export('userdb', 'users', outfn)

    def _parse_args(self):
        curdir = os.path.dirname(os.path.abspath(__file__))
        dir_default = os.path.join(curdir, 'backup')
        from argparse import ArgumentParser
        descr = 'Data Backup for LMFDB'
        epilog = 'LMFDB: www.lmfdb.org – github.org/LMFDB'
        parser = ArgumentParser(description=descr, epilog=epilog)

        parser.add_argument('what',
                            nargs='+',
                            help='what should be backed up?',
                            choices=BACKUPS)

        parser.add_argument('-o', '--out',
                            dest="dir",
                            help='the target base directory for all backups. the default is "%(default)s",',
                            default=dir_default)

        parser.add_argument('--dbhost',
                            help='the hostname of the database, default: %(default)s',
                            default="localhost")

        parser.add_argument('--dbport',
                            help="the port number of the database, default: %(default)s",
                            default=37010)

        return parser.parse_args()

if __name__ == "__main__":
    bkb = LMFBDBackup()
