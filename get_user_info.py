import json
import time
from datetime import datetime
from typing import Union, Optional, Any

import pandas as pd
import numpy as np
from httpx import Client
from loguru import logger

from settings import settings
from utils import fill_blank_dataframe_from_dict

"""
activities,

about,

blacklisted,

blacklisted_by_me,

books,

bdate,

can_be_invited_group,

can_post,

can_see_all_posts,

can_see_audio,

can_send_friend_request,

can_write_private_message,

career,

common_count,

connections,

contacts,

city,

country,

crop_photo,

domain,

education,

exports,

followers_count,

friend_status,

has_photo,

has_mobile,

home_town,

photo_100,

photo_200,

photo_200_orig,

photo_400_orig,

photo_50,

sex,

site,

schools,

screen_name,

status,

verified,

games,

interests,

is_favorite,

is_friend,

is_hidden_from_feed,

last_seen,

maiden_name,

military,

movies,

music,

nickname,

occupation,

online,

personal,

photo_id,

photo_max,

photo_max_orig,

quotes,

relation,

relatives,

timezone,

tv,

universities
"""

FIELDS = (
    "activities,about,blacklisted,blacklisted_by_me,books,bdate,can_be_invited_group,can_post,can_see_all_posts,"
    "can_see_audio,can_send_friend_request,can_write_private_message,career,common_count,connections,contacts,"
    "city,country,domain,education,exports,followers_count,friend_status,has_photo,has_mobile,home_town,sex,"
    "site,schools,screen_name,status,verified,games,interests,is_favorite,is_friend,is_hidden_from_feed,"
    "last_seen,maiden_name,movies,music,nickname,occupation,online,personal,photo_id,quotes,relation,"
    "relatives,timezone,tv,universities"
)


def skip_if_arg_none(func):
    def _wrapper(*args, **kwargs):
        if args[1] is not None:
            return func(*args, **kwargs)

    return _wrapper


class UserInfo:
    def __init__(
        self,
        users_ids: Union[list[str], None] = None,
        bots_ids: Union[list[str], None] = None,
        group_people_ids: Union[list[str], None] = None,
    ) -> None:
        self._client = Client(base_url=settings.BASE_URL)
        self._request_data = {
            "access_token": settings.ACCESS_TOKEN,
            "v": settings.VERSION,
        }
        self._users_ids = users_ids
        self._bots_ids = bots_ids
        self._group_people_ids = group_people_ids

    def get_users_info(self) -> tuple[Optional[Any], Optional[Any], Optional[Any]]:
        users_df = self._get_user_info(self._users_ids)
        bots_df = self._get_user_info(self._bots_ids)
        group_people_df = self._get_user_info(self._group_people_ids)
        self._get_user_friends(users_df)
        self._get_user_friends(bots_df)
        self._get_user_friends(group_people_df)
        self._get_user_posts(users_df)
        self._get_user_posts(bots_df)
        self._get_user_posts(group_people_df)
        self._get_user_subscriptions_count(users_df)
        self._get_user_subscriptions_count(bots_df)
        self._get_user_subscriptions_count(group_people_df)

        if users_df is not None:
            users_df["bots"] = 0
        if bots_df is not None:
            bots_df["bots"] = 1
        if group_people_df is not None:
            group_people_df["bots"] = 2

        return users_df, bots_df, group_people_df

    @skip_if_arg_none
    def _get_user_info(self, users_ids: list[str]) -> pd.DataFrame:
        # for index in range(1):
        users_info = []
        res = self._client.post(
            "/users.get",
            data={
                **self._request_data,
                # "user_ids": ','.join(users_ids[index*100:(index+1)*100]),
                "user_ids": ",".join(users_ids),
                "fields": FIELDS,
            },
        )
        res_json = res.json()
        logger.debug(f"Request users.get: {res_json}")
        users_info.extend(res_json["response"])
        time.sleep(settings.SLEEP_TIME_IN_SECONDS)
        return fill_blank_dataframe_from_dict(users_info)

    @skip_if_arg_none
    def _get_user_friends(self, df: pd.DataFrame):
        for index, user in df.iterrows():
            res = self._client.post(
                "/friends.get",
                data={
                    **self._request_data,
                    "user_id": user["id"],
                },
            )
            res_json = res.json()
            # logger.debug(f"Request friends.get: {res_json}")
            if res_json.get("error"):
                friends_count = -1
                friends_list = []
            else:
                res_json = res_json["response"]
                friends_count = res_json["count"]
                friends_list = res_json["items"]
            df.at[index, "friends_count"] = friends_count
            df.at[index, "friends_list"] = str(friends_list) if friends_list else np.nan
            time.sleep(settings.SLEEP_TIME_IN_SECONDS)

    def __extract_likes_reposts_comments_posts(self, res, df, index):
        user_posts = json.loads(res.text)
        # Проверка на наличие поля items в ответе
        if "response" in user_posts and "items" in user_posts["response"]:
            # Инициализация счетчиков
            likes_count = 0
            reposts_count = 0
            comments_count = 0
            for post in user_posts["response"]["items"]:
                if likes := post.get("likes"):
                    likes_count += likes["count"]
                if reposts := post.get("reposts"):
                    reposts_count += reposts["count"]
                if comments := post.get("comments"):
                    comments_count += comments["count"]

            df.at[index, "likes_100_last_post_count"] = likes_count
            df.at[index, "reposts_100_last_post_count"] = reposts_count
            df.at[index, "comments_100_last_post_count"] = comments_count

            # print("Количество лайков за последние 100 постов:", likes_count)
            # print("Количество репостов за последние 100 постов:", reposts_count)
            # print("Количество комментариев за последние 100 постов:", comments_count)
        # else:
        #     print("Список постов пользователя недоступен")


    def __extract_posts_frequency(self, res, df, index):
        user_posts = json.loads(res.text)
        if "response" in user_posts and "items" in user_posts["response"]:
            posts = user_posts["response"]["items"]

            # Список временных меток публикаций постов
            post_dates = []
            for post in posts:
                post_dates.append(post["date"])
            # Среднее время между постами
            time_diffs = []
            for i in range(len(post_dates) - 1):
                time_diffs.append(post_dates[i] - post_dates[i + 1])
            if time_diffs:
                avg_time_posts_diff = sum(time_diffs) / len(time_diffs)
            else:
                avg_time_posts_diff = -1
            df.at[index, "avg_time_posts_diff"] = avg_time_posts_diff
            # print("Среднее время между постами:", avg_time_posts_diff, "сек.")

            # Частота добавления контента в разное время суток
            post_times = []
            for post in posts:
                post_time = datetime.fromtimestamp(post["date"]).time()
                post_times.append(post_time)

            morning_posts = [
                time
                for time in post_times
                if datetime.strptime("06:00", "%H:%M").time()
                   <= time
                   <= datetime.strptime("12:00", "%H:%M").time()
            ]
            afternoon_posts = [
                time
                for time in post_times
                if datetime.strptime("12:00", "%H:%M").time()
                   < time
                   <= datetime.strptime("18:00", "%H:%M").time()
            ]
            evening_posts = [
                time
                for time in post_times
                if datetime.strptime("18:00", "%H:%M").time()
                   < time
                   <= datetime.strptime("00:00", "%H:%M").time()
            ]
            night_posts = [
                time
                for time in post_times
                if datetime.strptime("00:00", "%H:%M").time()
                   < time
                   <= datetime.strptime("06:00", "%H:%M").time()
            ]

            df.at[index, "morning_posts"] = len(morning_posts)
            df.at[index, "afternoon_posts"] = len(afternoon_posts)
            df.at[index, "evening_posts"] = len(evening_posts)
            df.at[index, "night_posts"] = len(night_posts)

    @skip_if_arg_none
    def _get_user_posts(self, df: pd.DataFrame):
        for index, row in df.iterrows():
            res = self._client.post(
                "/wall.get",
                data={
                    **self._request_data,
                    "domain": row["domain"],
                    "count": 100,
                },
            )
            res_json = res.json()
            logger.debug(f"Request wall.get: {res_json}")
            if response := res_json.get("response"):
                print(f"#########{response['count']}#########")
                df.at[index, "posts_count"] = response["count"]
            if response := res_json.get("error"):
                if response["error_code"] == 30:
                    print("!!! -1")
                    df.at[index, "posts_count"] = -1
                elif response["error_code"] == 18:
                    print("!!! -2")
                    df.at[index, "posts_count"] = -2
                else:
                    print(row["domain"])
                    print(response["error_msg"])
                    print("!!! -3")
                    df.at[index, "posts_count"] = -3

            self.__extract_posts_frequency(res, df, index)
            self.__extract_likes_reposts_comments_posts(res, df, index)
            time.sleep(settings.SLEEP_TIME_IN_SECONDS)

    @skip_if_arg_none
    def _get_user_subscriptions_count(self, df: pd.DataFrame):
        for index, row in df.iterrows():
            res = self._client.post(
                "/users.getSubscriptions",
                data={
                    **self._request_data,
                    "user_id": row["id"],
                },
            )
            res_json = res.json()
            logger.debug(f"Request users.getSubscriptions: {res_json}")

            if response := res_json.get("response"):
                print(f"######### users {response['users']['count']}#########")
                print(f"######### groups {response['groups']['count']}#########")
                df.at[index, "users_subscriptions_count"] = response["users"]["count"]
                df.at[index, "groups_subscriptions_count"] = response["groups"]["count"]
            if response := res_json.get("error"):
                if response["error_code"] == 30:
                    print("!!! -1")
                    df.at[index, "users_subscriptions_count"] = -1
                    df.at[index, "groups_subscriptions_count"] = -1
                elif response["error_code"] == 18:
                    print("!!! -2")
                    df.at[index, "users_subscriptions_count"] = -2
                    df.at[index, "groups_subscriptions_count"] = -2
                else:
                    print(row["id"])
                    print(response["error_msg"])
                    print("!!! -3")
                    df.at[index, "users_subscriptions_count"] = -3
                    df.at[index, "groups_subscriptions_count"] = -3

            time.sleep(settings.SLEEP_TIME_IN_SECONDS)
