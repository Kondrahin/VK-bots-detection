import ast
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

BOOL_COLUMNS = [
    "interests",
    "books",
    "tv",
    "quotes",
    "about",
    "games",
    "movies",
    "activities",
    "music",
    "mobile_phone",
    "home_phone",
    "site",
    "status",
    "university",
    "university_name",
    "faculty",
    "faculty_name",
    "graduation",
    "home_town",
    "relation",
    "personal",
    "universities",
    "schools",
    "occupation",
    "education_form",
    "education_status",
    "relation_partner",
    "skype",
    "twitter",
    "livejournal",
    "instagram",
    "facebook",
    "facebook_name",
]


class CreateFeatures:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def generate_features(self):
        for index, row in self.df.iterrows():
            self._split_features(index, row)
            self._create_relative_in_friends_bool_feature(index, row)
            self._create_partner_in_friends_bool_feature(index, row)
            self._create_if_change_nickname_bool_feature(index, row)
            self._calculate_subscriptions_followers_coef(index, row)
        self._create_general_bool_features()
        self._calculate_subscriptions_followers_norm_coef()
        return self.df

    def _split_features(self, index, row):
        self.df.at[index, "city_id"] = (
            ast.literal_eval(row["city"])["id"] if not pd.isna(row["city"]) else 0
        )
        self.df.at[index, "city_title"] = (
            ast.literal_eval(row["city"])["title"] if not pd.isna(row["city"]) else 0
        )
        self.df.at[index, "country_id"] = (
            ast.literal_eval(row["country"])["id"] if not pd.isna(row["country"]) else 0
        )
        self.df.at[index, "country_title"] = (
            ast.literal_eval(row["country"])["title"]
            if not pd.isna(row["country"])
            else 0
        )
        self.df.at[index, "last_seen_platform"] = (
            ast.literal_eval(row["last_seen"]).get("platform")
            if not pd.isna(row["last_seen"])
            else 0
        )
        self.df.at[index, "last_seen_time"] = (
            ast.literal_eval(row["last_seen"])["time"]
            if not pd.isna(row["last_seen"])
            else 0
        )
        self.df.at[index, "can_be_invited_group"] = (
            0.0 if row["can_be_invited_group"] == "False" else 1.0
        )

    def _create_general_bool_features(self):
        for index, user in self.df.isnull().iterrows():
            for bool_column in BOOL_COLUMNS:
                if user.get(bool_column):
                    self.df.at[index, f"{bool_column}_bool"] = 0
                else:
                    self.df.at[index, f"{bool_column}_bool"] = 1

    def _create_relative_in_friends_bool_feature(self, index, row):
        self.df.at[index, "relatives_in_friends_bool"] = 0
        if (
            type(row["relatives"]) is not float
            and type(row["friends_list"]) is not float
        ):
            relatives: list[dict] = row["relatives"]
            friends_list: list[int] = row["friends_list"]
            relatives_ids = [
                relative_dict["id"] for relative_dict in ast.literal_eval(relatives)
            ]
            if any(str(relative) in friends_list for relative in relatives_ids):
                self.df.at[index, "relatives_in_friends_bool"] = 1

    def _create_partner_in_friends_bool_feature(self, index, row):
        self.df.at[index, "partner_in_friends_bool"] = 0
        if (
            type(row["relation_partner"]) is not float
            and type(row["friends_list"]) is not float
        ):
            partner = ast.literal_eval(row["relation_partner"])["id"]
            friends_list: list[int] = row["friends_list"]
            if str(partner) in friends_list:
                self.df.at[index, "partner_in_friends_bool"] = 1

    def _create_if_change_nickname_bool_feature(self, index, row):
        self.df.at[index, "change_nickname_bool"] = 1
        if row["domain"].startswith("id") and row["domain"].endswith(str(row["id"])):
            self.df.at[index, "change_nickname_bool"] = 0

    def _calculate_subscriptions_followers_coef(self, index, row):
        users_subscriptions_count = row["users_subscriptions_count"]
        followers_count = row["followers_count"]
        if users_subscriptions_count and followers_count:
            self.df.at[index, "subscriptions_followers_coef"] = (
                users_subscriptions_count / followers_count
            )
        elif users_subscriptions_count:
            self.df.at[index, "subscriptions_followers_coef"] = users_subscriptions_count
        elif followers_count:
            self.df.at[index, "subscriptions_followers_coef"] = 1 / followers_count
        else:
            self.df.at[index, "subscriptions_followers_coef"] = 0

    def _calculate_subscriptions_followers_norm_coef(self):
        scaler = MinMaxScaler()
        self.df["subscriptions_followers_coef_norm"] = scaler.fit_transform(
            self.df[["subscriptions_followers_coef"]]
        )
