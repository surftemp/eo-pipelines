# MIT License
#
# Copyright (c) 2022 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import os.path
import sqlite3
import logging


class TrackingDatabase:

    def __init__(self, path=None, run_id=None):
        self.logger = logging.getLogger("TrackingDatabase")
        self.path = path
        self.run_id = run_id
        if path is not None:
            create_schema = False
            if not os.path.exists(path):
                create_schema = True

            with sqlite3.connect(path) as db:

                if create_schema:
                    curs = db.cursor()
                    curs.execute("CREATE TABLE RUNS(RUN_ID STRING, START_TIME DATETIME, PRIMARY KEY (RUN_ID));")
                    curs.execute("CREATE TABLE TASKS(RUN_ID STRING, STAGE_ID STRING, TASK_ID STRING, TRY_NR INTEGER, QUEUE_TIME DATETIME, START_TIME DATETIME, END_TIME DATETIME, RESULT INTEGER, MESSAGE STRING, PRIMARY KEY (RUN_ID, STAGE_ID, TASK_ID, TRY_NR));")
                    db.commit()

                if self.run_id is not None:
                    # if any runs are recorded with the same ID, remove them
                    curs = db.cursor()
                    curs.execute("DELETE FROM RUNS WHERE RUN_ID = ?",[run_id])
                    curs.execute("DELETE FROM TASKS WHERE RUN_ID = ?", [run_id])
                    curs.execute("INSERT INTO RUNS(RUN_ID, START_TIME) VALUES(?,?)",[self.run_id,datetime.datetime.now()])
                    db.commit()

    def is_active(self):
        return self.path is not None

    def track_task_queued(self, stage_id, task_id, try_nr):
        if self.path and self.run_id:
            with sqlite3.connect(self.path) as db:
                try:
                    curs = db.cursor()
                    curs.execute("INSERT INTO TASKS(RUN_ID, STAGE_ID, TASK_ID, TRY_NR, QUEUE_TIME) VALUES(?,?,?,?,?)",[self.run_id, stage_id, task_id, try_nr, datetime.datetime.now()])
                    db.commit()
                except sqlite3.IntegrityError:
                    self.logger.exception("Row already exists in tracking database")
                except:
                    self.logger.exception("Tracking database update failed")

    def track_task_start(self, stage_id, task_id, try_nr):
        if self.path and self.run_id:
            with sqlite3.connect(self.path) as db:
                try:
                    curs = db.cursor()
                    curs.execute("UPDATE TASKS SET START_TIME=? WHERE RUN_ID=? AND STAGE_ID=? AND TASK_ID=? AND TRY_NR=? ;",[datetime.datetime.now(), self.run_id, stage_id, task_id, try_nr])
                    db.commit()
                except:
                    self.logger.exception("Tracking database update failed")

    def track_task_end(self, stage_id, task_id, try_nr, result, message=""):
        if self.path and self.run_id:
            with sqlite3.connect(self.path) as db:
                try:
                    curs = db.cursor()
                    curs.execute("UPDATE TASKS SET END_TIME=?, RESULT=?, MESSAGE=? WHERE RUN_ID=? AND STAGE_ID=? AND TASK_ID=? AND TRY_NR=? ;",[datetime.datetime.now(), result, message, self.run_id, stage_id, task_id, try_nr])
                    db.commit()
                except:
                    self.logger.exception("Tracking database update failed")

    def list_runs(self):
        with sqlite3.connect(self.path) as db:
            curs = db.cursor()
            return curs.execute("SELECT * FROM RUNS")

    def list_tasks(self, run_id=None, stage_id=None):
        with sqlite3.connect(self.path) as db:
            curs = db.cursor()
            if stage_id:
                if run_id:
                    results = curs.execute(
                        "SELECT * FROM TASKS WHERE RUN_ID = ? AND STAGE_ID = ? ORDER BY STAGE_ID, TASK_ID, TRY_NR", [run_id, stage_id])
                else:
                    results = curs.execute("SELECT * FROM TASKS WHERE STAGE_ID = ? ORDER BY RUN_ID, STAGE_ID, TASK_ID, TRY_NR",[stage_id])
            else:
                if run_id:
                    results = curs.execute(
                        "SELECT * FROM TASKS WHERE RUN_ID = ? ORDER BY STAGE_ID, TASK_ID, TRY_NR",[run_id])
                else:
                    results = curs.execute("SELECT * FROM TASKS ORDER BY RUN_ID, STAGE_ID, TASK_ID, TRY_NR")
            return results

