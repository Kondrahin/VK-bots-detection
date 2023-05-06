import ast

import numpy as np
import pandas as pd
import math

from app.create_features import BOOL_COLUMNS

FEATURE_COLUMNS = [
    "has_photo",
    "has_mobile",
    "is_friend",
    "can_post",
    "can_see_all_posts",
    "can_see_audio",
    "can_write_private_message",
    "can_send_friend_request",
    "can_be_invited_group",
    "followers_count",
    "blacklisted",
    "blacklisted_by_me",
    "is_favorite",
    "is_hidden_from_feed",
    "common_count",
    "university",
    "faculty",
    "graduation",
    "relation",
    "verified",
    "deactivated",
    "friend_status",
    "can_access_closed",
    "is_closed",
    "city_id",
    "country_id",
    "last_seen_platform",
    "last_seen_time",
    "interests_bool",
    "books_bool",
    "tv_bool",
    "quotes_bool",
    "about_bool",
    "games_bool",
    "movies_bool",
    "activities_bool",
    "music_bool",
    "mobile_phone_bool",
    "home_phone_bool",
    "site_bool",
    "status_bool",
    "university_bool",
    "university_name_bool",
    "faculty_bool",
    "faculty_name_bool",
    "graduation_bool",
    "home_town_bool",
    "relation_bool",
    "personal_bool",
    "universities_bool",
    "schools_bool",
    "occupation_bool",
    "education_form_bool",
    "education_status_bool",
    "relation_partner_bool",
    "skype_bool",
    "twitter_bool",
    "livejournal_bool",
    "instagram_bool",
    "facebook_bool",
    "facebook_name_bool",
    "relatives_in_friends_bool",
    "change_nickname_bool",
    "partner_in_friends_bool",
]
COUNTERS_COLUMNS = [
    "albums",
    "audios",
    "followers",
    "gifts",
    "pages",
    "photos",
    "subscriptions",
    "videos",
    "video_playlists",
]


class Preprocessing:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def _delete_empty_lists_in_df(self):
        columns = self.df.columns.tolist()
        for index, row in self.df.iterrows():
            for c in columns:
                if row[c] == "[]" or row[c] == []:
                    self.df.at[index, c] = np.nan

    def _delete_zeros_in_columns(self):
        for index, row in self.df.iterrows():
            for c in BOOL_COLUMNS:
                if row[c] == 0.0 or row[c] == "":
                    self.df.at[index, c] = np.nan

    def _deactivated_column_to_int(self):
        for index, row in self.df.iterrows():
            if row["deactivated"] == "banned":
                self.df.at[index, "deactivated"] = 2
            elif row["deactivated"] == "deleted":
                self.df.at[index, "deactivated"] = 1
            else:
                self.df.at[index, "deactivated"] = 0

    def _counters_dict_to_features(self):
        for index, row in self.df.iterrows():
            counters = row["counters"]
            for field in COUNTERS_COLUMNS:
                if not math.isnan(counters):
                    self.df.at[index, f"{field}_count"] = ast.literal_eval(counters)[field]
                else:
                    self.df.at[index, f"{field}_count"] = 0

    def preprocessing(self) -> pd.DataFrame:
        self._deactivated_column_to_int()
        self._delete_empty_lists_in_df()
        self._delete_zeros_in_columns()
        self._counters_dict_to_features()

        return self.df
