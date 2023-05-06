from app.create_features import CreateFeatures
from app.get_user_info import UserInfo
from app.preprocessing import Preprocessing


class BuildDataset:
    def __init__(self, users_ids=None, bots_ids=None):
        self.users_ids = users_ids
        self.bots_ids = bots_ids

    def build_dataset(self):
        user_info = UserInfo(users_ids=self.users_ids, bots_ids=self.bots_ids)
        users_df, bots_df = user_info.get_users_info()
        if users_df is not None:
            users_prep = Preprocessing(users_df)
            users_prep_df = users_prep.preprocessing()

            users_features = CreateFeatures(users_prep_df)
            users_features = users_features.generate_features()

            users_features.to_csv("output/users_df.csv")
        if bots_df is not None:
            bots_prep = Preprocessing(bots_df)
            bots_prep_df = bots_prep.preprocessing()

            bots_features = CreateFeatures(bots_prep_df)
            bots_features = bots_features.generate_features()

            bots_features.to_csv("output/bots_df.csv")
