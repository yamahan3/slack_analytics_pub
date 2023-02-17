from slack_sdk.web import WebClient
import pandas as pd
import os
from dotenv import load_dotenv
import re
import collections
import json
from datetime import datetime, timedelta
from dateutil import tz

# 取得したトークン
load_dotenv()
SLACK_API_TOKEN = os.environ['SLACK_API_TOKEN']

class slack_analytics():
    # デフォルト現在時間
    default_latest=datetime.now(tz.gettz("Asia/Tokyo")).timestamp()

    def __init__(self, token):
        self.client = WebClient(token=token)

        # WorkSpaseの全ユーザ―の取得
        # privateを含める場合：users_list(types="public_channel,private_channel")
        self.users_list = self.client.users_list()['members']

        # WorkSpaceのすべての公開チャンネルを取得(アーカイブ済みを除く)
        self.channels_list = self.client.conversations_list(exclude_archived=True, limit=1000)[
            'channels']

        # WorkSpaceの全チャンネルのIDを取得
        channel_ids = [c.get("id") for c in self.channels_list]
        # 全メッセージを取得
        self.all_messages = {}
        for channel_id in channel_ids:
            messages = self.client.conversations_history(
                channel=channel_id, limit=10000)['messages']
            self.all_messages[channel_id] = []
            # チャンネルIDをキーとしてメッセージを格納
            for m in messages:
                # botメッセージは読み飛ばす
                if "subtype" in m.keys() and m["subtype"] == "bot_message":
                    continue
                self.all_messages[channel_id].append(m)

        # 全メッセージを保存
        with open('./data/messages.json', 'w') as f:
            json.dump(self.all_messages, f, ensure_ascii=False, indent=2)

    # 指定されたチャンネルの全メッセージを取得
    def get_conversations_history(self, channel_ids, latest=default_latest, oldest=0):
        # 会話内容格納用
        conversations_history = []

        # チャンネルごとにメッセージを取得
        for channel_id in channel_ids:
            for m in self.all_messages[channel_id]:
                # 期間指定
                if oldest <= float(m["ts"]) and float(m["ts"]) <= latest:
                    conversations_history.append(m)

        return conversations_history
    
    # 全チャンネルIDを取得
    def get_channel_ids(self):
        return [c.get("id") for c in self.channels_list]

    # 全チャンネル名を取得
    def get_channel_names(self):
        return [c.get("name") for c in self.channels_list]
    
    # 特定チャンネルのIDを取得
    def get_channel_messages(self, channel_id, latest=default_latest, oldest=0):
        messages = self.get_conversations_history([channel_id], latest, oldest)
        return messages
    
    # 特定チャンネルでメッセージを発信したユーザを取得
    def get_channel_message_users(self, channel_id, latest=default_latest, oldest=0):
        messages = self.get_channel_messages(channel_id, latest, oldest)
        return [m.get("user") for m in messages]
    
    # 特定チャンネルでリアクションしたユーザを取得
    def get_channel_reaction_users(self, channel_id, latest=default_latest, oldest=0):
        messages = self.get_channel_messages(channel_id, latest, oldest)
        reaction_users = []
        for m in messages:
            # メッセージにリアクションがあれば
            if 'reactions' in m.keys():
                # リアクションの種類ごとに取得
                for reaction in m['reactions']:
                    reaction_users.extend(reaction['users'])
        return reaction_users

    # チャンネルごとの情報をDFにまとめる
    def get_channel_data(self, latest=default_latest, oldest=0):
        channel_ids = self.get_channel_ids()
        channel_names = self.get_channel_names()
        channel_id_num = len(channel_ids)

        channel_df = pd.DataFrame(data={
            'channel_name': channel_names,
            'action_people': [0] * channel_id_num,
            'message_people': [0] * channel_id_num,
            'reaction_people': [0] * channel_id_num,
            'action_num': [0] * channel_id_num,
            'message_num': [0] * channel_id_num,
            'reaction_num': [0] * channel_id_num,
        }, index=channel_ids)

        for channel_id in channel_ids:
            # チャンネルごとのメッセージ
            messages = self.get_channel_messages(channel_id, latest, oldest)
            # メッセージ数
            channel_df.at[channel_id, "message_num"] = len(messages)

            message_users = self.get_channel_message_users(channel_id, latest, oldest)  # メッセージしたユーザ
            reaction_users = self.get_channel_reaction_users(channel_id, latest, oldest)  # リアクションしたユーザ

            channel_df.at[channel_id, 'message_people'] = len(set(message_users))
            channel_df.at[channel_id, 'reaction_people'] = len(set(reaction_users))
            channel_df.at[channel_id, 'action_people'] = len(set(message_users + reaction_users))

            channel_df.at[channel_id, 'reaction_num'] = len(reaction_users)
            channel_df.at[channel_id, 'action_num'] = channel_df.at[channel_id,'message_num'] + channel_df.at[channel_id, 'reaction_num']

        return channel_df
    
    # 全ユーザのIDを取得
    def get_user_ids(self):
        user_ids = []  # ユーザID
        for user in self.users_list:
            # Botもしくは削除済みのユーザは含めない
            if user.get("is_bot") == False and user.get("deleted") == False:
                user_ids.append(user.get("id"))
        return user_ids
    
    # 全ユーザの名前を取得
    def get_user_names(self):
        user_names = []  # ユーザ名
        for user in self.users_list:
            # Botもしくは削除済みのユーザは含めない
            if user.get("is_bot") == False and user.get("deleted") == False:
                user_names.append(user.get("real_name"))
        return user_names
    
    # IDから名前を取得する関数
    def get_user_id_to_name(self, user_id):
        for user in self.users_list:
            if user.get("id") == user_id:
                return user.get("real_name")

    # 全ユーザのアイコンを取得
    def get_user_images(self):
        user_images = []  # ユーザアイコン
        for user in self.users_list:
            # Botもしくは削除済みのユーザは含めない
            if user.get("is_bot") == False and user.get("deleted") == False:
                # オリジナル画像がない場合はデフォルト設定のアイコンの大きいものを取得
                user_images.append(user.get("profile").get(
                    "image_original", user.get("profile").get("image_512")))
        return user_images
    
    # 特定のユーザのメッセージのみ取得
    def get_user_messages(self, user_id, latest=default_latest, oldest=0):
        channel_ids = self.get_channel_ids() # 全チャンネルID
        all_messages = self.get_conversations_history(channel_ids, latest, oldest)
        user_messages = []

        for m in all_messages:
            # 自己発信を読み取る
            if user_id == m['user']:
                user_messages.append(m)
        return user_messages

    # 特定のユーザのメンションしたユーザを取得
    def get_user_mentions(self, user_id, latest=default_latest, oldest=0):
        user_mentions = []
        user_messages = self.get_user_messages(user_id, latest, oldest)
        for m in user_messages:
            # メッセージからメンションユーザー部分だけ読み取る
            receive_mention_user = re.findall("(?<=<@).+?(?=>)", m["text"])
            for user in receive_mention_user:
                # メッセージを発信した人以外がメンションされていた場合
                if user != m["user"]:
                    user_mentions.append(user)
        return user_mentions
    
    # 特定のユーザがメンションされたユーザを取得
    def get_user_receive_mentions(self, user_id, latest=default_latest, oldest=0):
        channel_ids = self.get_channel_ids() # 全チャンネルID
        all_messages = self.get_conversations_history(channel_ids, latest, oldest)
        user_mentions = []
        for m in all_messages:
            # 自己発信以外を読み取る
            if user_id != m['user']:
                # メッセージからメンションユーザー部分だけ読み取る
                receive_mention_user = re.findall("(?<=<@).+?(?=>)", m["text"])
                for user in receive_mention_user:
                    # メッセージ内でメンションされていた場合
                    if user == user_id:
                        user_mentions.append(m['user'])
        return user_mentions

    # 特定のユーザがリアクションしたユーザを取得
    def get_user_reactions(self, user_id, latest=default_latest, oldest=0):
        channel_ids = self.get_channel_ids() # 全チャンネルID
        reaction_users = []
        all_messages = self.get_conversations_history(channel_ids, latest, oldest)
        for m in all_messages:
            # メッセージにリアクションがあった場合
            if 'reactions' in m.keys() and user_id != m['user']:
                # リアクションの種類ごとに計測
                for reaction in m['reactions']:
                    # 特定のユーザがリアクションした場合
                    if user_id in reaction['users']:
                        reaction_users.append(m['user'])
        return reaction_users

    # 特定のユーザがリアクションされたユーザを取得
    def get_user_receive_reactions(self, user_id, latest=default_latest, oldest=0):
        user_receive_reactions = []
        user_messages = self.get_user_messages(user_id, latest, oldest)
        for m in user_messages:
            # メッセージにリアクションがあった場合
            if 'reactions' in m.keys():
                # リアクションの種類ごとに計測
                for reaction in m['reactions']:
                    user_receive_reactions.extend(reaction['users'])
        return user_receive_reactions
                

    def get_user_data(self, latest=default_latest, oldest=0):
        user_ids = self.get_user_ids()  # ユーザID
        user_names = self.get_user_names()  # ユーザ名
        user_images = self.get_user_images()  # ユーザアイコン

        user_id_num = len(user_ids)  # ユーザID数
        channel_ids = self.get_channel_ids() # 全チャンネルID

        # 作成目標のデータフレーム
        user_df = pd.DataFrame(data={
            "user_name": user_names,
            "user_image": user_images,
            "action_num": [0]*user_id_num,
            "message_num": [0]*user_id_num,
            "reaction_num": [0]*user_id_num,
            "mention_people": [0]*user_id_num,
            "reaction_people": [0]*user_id_num,
            "receive_reaction_num": [0]*user_id_num,
            "receive_reaction_people": [0]*user_id_num,
            "receive_mention_num": [0]*user_id_num,
            "receive_mention_people": [0]*user_id_num,
        }, index=user_ids)

        message_users = []
        reaction_users = []
        for channel_id in channel_ids:
            message_users.extend(self.get_channel_message_users(channel_id, latest, oldest))
            reaction_users.extend(self.get_channel_reaction_users(channel_id, latest, oldest))

        message_users_count = collections.Counter(message_users)
        reaction_users_count = collections.Counter(reaction_users)

        for user_id in user_ids:
            # ユーザが発信したメッセージ数
            message_num = message_users_count[user_id]

            # ユーザがリアクションした数
            reaction_num = reaction_users_count[user_id]

            # ユーザのアクション数
            action_num = message_num + reaction_num

            # ユーザがメンションしたユーザ
            user_mentions = self.get_user_mentions(user_id, latest, oldest)
            mention_people = len(set(user_mentions))

            # ユーザがリアクションしたユーザ
            user_reactions = self.get_user_reactions(user_id, latest, oldest)
            # ユニークユーザ数
            reaction_people = len(set(user_reactions))

            # ユーザがリアクションされたユーザ
            receive_reaction_users = self.get_user_receive_reactions(user_id, latest, oldest)
            receive_reaction_num = len(receive_reaction_users)
            # ユニークユーザ数
            receive_reaction_people = len(set(receive_reaction_users))

            # ユーザがメンションされたユーザを取得
            receive_mention_users = self.get_user_receive_mentions(user_id, latest, oldest)
            receive_mention_num = len(receive_mention_users)
            # ユニークユーザ数
            receive_mention_people = len(set(receive_mention_users))

            # DF生成
            user_df.at[user_id, 'action_num'] = action_num
            user_df.at[user_id, 'message_num'] = message_num
            user_df.at[user_id, 'reaction_num'] = reaction_num
            user_df.at[user_id, 'mention_people'] = mention_people
            user_df.at[user_id, 'reaction_people'] = reaction_people
            user_df.at[user_id, 'receive_reaction_num'] = receive_reaction_num
            user_df.at[user_id, 'receive_reaction_people'] = receive_reaction_people
            user_df.at[user_id, 'receive_mention_num'] = receive_mention_num
            user_df.at[user_id, 'receive_mention_people'] = receive_mention_people

        return user_df

    # oldest時間（UTC）から現在の時間までのアクション人数のグラフを生成
    def action_num_graph(self, oldest):
        channel_ids = [c.get("id") for c in self.channels_list]
        # 今日の日付+1を取得
        JST = tz.gettz("Asia/Tokyo")
        latest = datetime.now(JST)

        # 指定期間の全メッセージを取得
        messages = self.get_conversations_history(
                channel_ids, latest, oldest)

        graph_x_date = []
        graph_y_message = []
        graph_y_reaction = []
            
        for date in self.date_range(oldest, latest):
            # メッセージ数
            message_num = 0
            # リアクション数
            reaction_num = 0

            # datetime -> string
            graph_x_date.append(date.strftime('%Y-%m-%d'))
            
            # タイムスタンプに変換
            latest_timestamp = (date + timedelta(days=+1)).timestamp()
            # oldest_timestamp = date.timestamp()
            # print(oldest_timestamp)
            for message in messages:
                # 一日を超えたらbreak
                if message["ts"] < latest_timestamp:
                    break

                message_num+=1

                # メッセージにリアクションがあった場合
                if 'reactions' in message.keys():
                    for reaction in message['reactions']:
                        # リアクション数を計測
                        reaction_num += reaction['count']

                graph_y_message.append(message_num)
                graph_y_reaction.append(reaction_num)

        print(graph_x_date)
        print(graph_y_message)
        print(graph_y_reaction)

    # ループ用に日付ごとのrangeを生成(降順)
    def date_range(self, stop, start, step=timedelta(days=1)):
        current = start
        while current >= stop:
            yield current
            current -= step

'''
# 現在の時刻をエポック秒で取得
JST = tz.gettz("Asia/Tokyo")
latest_datetime = datetime.now(JST)

# JST -> UTC
oldest_date = "2023-01-01"
oldest_date = oldest_date + " 00:00:00+0900"
oldest_datetime = datetime.strptime(oldest_date, '%Y-%m-%d %H:%M:%S%z')
'''

analytics = slack_analytics(SLACK_API_TOKEN)

channel_df = analytics.get_channel_data()
user_df = analytics.get_user_data()

# print(channel_df)
# print(user_df)
channel_df.to_csv('data/channel_out.csv')
user_df.to_csv('data/user_out.csv')


