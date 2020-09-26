import pandas as pd
import plotly.express as px
from tqdm import tqdm


class LogDeleteEvaluator(object):

    def __init__(self, event: tuple):
        self._event = event
        self.actions = []
        self.after = []
        self.after4scatter = None

        self.figs = None

        self.tick = self._event[1]
        self.event = self._event[2]
        self.num_deleted_files = -1
        self.total_size_deleted_files = -1.
        self.total_num_req_after_delete = -1

        self.on_delete_cache_size = self._event[3]
        self.on_delete_cache_occupancy = self._event[4]

    def add(self, action: tuple):
        self.actions.append(action)

    def trace(self, after_action: tuple):
        self.after.append(after_action)

    def prepare(self, columns):
        self.actions = pd.DataFrame(self.actions, columns=columns)
        self.actions.set_index('Index', inplace=True)
        self.actions.reset_index(inplace=True, drop=True)
        self.after = pd.DataFrame(self.after, columns=columns)
        self.after.set_index('Index', inplace=True)
        self.after.reset_index(inplace=True, drop=True)
        self.after4scatter = self.after.copy()
        self.after4scatter = self.after4scatter.loc[self.after4scatter['action or event'] == "ADD"]

        self._fix_delta_t_max()

        self.num_deleted_files = self._get_num_deleted_files()
        self.total_size_deleted_files = self._get_total_size_deleted_files()
        self.total_num_req_after_delete = self._get_num_deleted_miss()

    @property
    def scatterActions(self):
        return px.scatter_3d(
            self.actions,
            x='num req',
            y='size',
            z='filename',
            color='delta t',
            size='size',
            opacity=0.9,
        )

    @property
    def scatterAfter(self):
        return px.scatter_3d(
            self.after[self.after.size != -1.],
            x='num req',
            y='size',
            z='filename',
            color='delta t',
            size='size',
            opacity=0.9,
        )

    @property
    def histActionNumReq(self):
        return px.histogram(self.actions, x='num req')

    @property
    def histActionSize(self):
        return px.histogram(self.actions, x='Size')

    @property
    def histActionDeltaT(self):
        return px.histogram(self.actions, x='delta t')

    def _get_num_deleted_files(self):
        return len(set(self.actions.filename))

    def _get_total_size_deleted_files(self):
        return self.actions.size.sum()

    def _get_num_deleted_miss(self):
        files = set(self.after.filename) & set(self.actions.filename)
        tot = 0
        if len(files) > 0:
            counts = self.after.filename[self.after['action or event'] == "MISS"].value_counts(
            )
            tot = sum(counts[file_] for file_ in files if file_ in counts)
        return tot

    def _fix_delta_t_max(self):
        cur_max = self.actions['delta t'].max()
        selectRows = self.actions['delta t'] == cur_max
        self.actions.loc[selectRows, 'delta t'] = -1.
        new_max = self.actions['delta t'].max()
        selectRows = self.actions['delta t'] == -1.
        self.actions.loc[selectRows, 'delta t'] = new_max * 2.

        cur_max = self.after['delta t'].max()
        selectRows = (self.after['delta t'] == cur_max) & (
            self.after.size != -1.)
        self.after.loc[selectRows, 'delta t'] = -1.
        new_max = self.after['delta t'].max()
        selectRows = (self.after['delta t'] == -1.) & (self.after.size != -1.)
        self.after.loc[selectRows, 'delta t'] = new_max * 2.


def parse_sim_log(log_df: 'pd.DataFrame', target: str = "AFTERDELETE"):
    if target == "AFTERDELETE":
        curLog = None
        state = "AFTERDELETE"

        # print(file_, log_df)

        # log_df = log_df[:1000000]
        for row in tqdm(log_df.itertuples(), desc="Parse log",
                        total=len(log_df.index), position=2):
            event = row[2]

            if state == "AFTERDELETE":
                if event in ["ONFREE", "ONDAYEND", "ONK", "FORCEDCALL", "FREE"]:
                    if curLog is not None:
                        curLog.prepare(['Index'] + list(log_df.columns))
                        yield curLog
                    curLog = LogDeleteEvaluator(row)
                    state = "DELETING"
                elif curLog is not None:
                    curLog.trace(row)

            elif state == "DELETING":
                if event in ["KEEP", "DELETE"]:
                    curLog.add(row)
                else:
                    state = "AFTERDELETE"
                    curLog.trace(row)
        else:
            curLog.prepare(['Index'] + list(log_df.columns))
            yield curLog

    elif target == "MISSFREQ":
        deleted_files = {}
        name2check = None

        freq_deleted = []
        freq_skip = []

        state = "AFTERDELETE"

        for row in tqdm(log_df.itertuples(), desc="Parse log",
                        total=len(log_df.index), position=2):
            event = row[2]
            filename = row[5]

            if event == "MISS":
                if state == "DELETING":
                    state = "AFTERDELETE"

                name2check = filename

                if filename in deleted_files:
                    freq_deleted.append(deleted_files[filename])
                    del deleted_files[filename]

            elif event == "DELETE":
                if state == "AFTERDELETE":
                    deleted_files = {}
                    state = "DELETING"

                freq = int(row[7])
                deleted_files[filename] = freq

            elif event == "SKIP":
                if state == "DELETING":
                    state = "AFTERDELETE"

                assert filename == name2check, f"SKIPPED filename is different... {filename}!={name2check}"

                freq = int(row[7])
                freq_skip.append(freq)

                name2check = None
            else:
                if state == "DELETING":
                    state = "AFTERDELETE"

        yield freq_deleted, freq_skip

    else:
        raise Exception(f"ERROR: target {target} not available...")
