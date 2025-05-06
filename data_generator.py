import random
import csv
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker for generating fake data
fake = Faker('zh_CN')  # Use Chinese locale for Faker to generate realistic Chinese names


# Function to generate a random Chinese name (2-4 characters)
def generate_chinese_name():
    first_names = ["张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"]
    last_names = ["伟", "芳", "娜", "敏", "静", "强", "磊", "洋", "军", "梅", "丽", "霞"]
    return random.choice(first_names) + random.choice(last_names)


# Function to generate diverse messages
def generate_chinese_message():
    messages = [
        "我每天都在控制着不去想你，对于未来我从不去奢求什么，只希望你心中永远有个我。",
        "每次和你说话，时间仿佛都静止了，我希望能永远陪伴在你身边。",
        "在最平凡的日子里，你的一个微笑能点亮我整天的心情。",
        "我不求你的一切，只希望你能在我身边，给我一个温暖的拥抱。",
        "每天想着你，虽然我们很远，但我相信我们的心是近的。",
        "你的一句话，一直萦绕在我耳边，感觉你就在我身边。",
        "即使世界再大，见你一面，所有的困难都能迎刃而解。",
        "爱你，是我这一生最幸运的事，愿我们能够永远在一起。",
        "没有你的日子，似乎少了点什么，我在等你回来。",
        "未来的路很长，但我相信只要有你在身边，一切都会变得美好。"
    ]
    return random.choice(messages)


# Updated function to generate simulated log data with phone type corresponding to OS
def generate_log_data(num_records):
    logs = []

    # Phone type distribution: More popular phone types have higher weight
    phone_types_android = ["小米 Redmi K30", "OnePlus 6", "VIVO X50", "Oppo Reno4"]
    phone_types_ios = ["Apple iPhone 10", "Apple iPhone 11", "Apple iPhone 12"]

    # Weighting factors for more popular phones
    popular_android = ["小米 Redmi K30", "OnePlus 6"]  # More popular Android phones
    popular_ios = ["Apple iPhone 10", "Apple iPhone 11"]  # More popular iPhones

    for _ in range(num_records):
        # Generate a random timestamp between 2025-01-01 and 2025-04-01
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 4, 1)
        delta = end_date - start_date
        random_date = start_date + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
        timestamp = random_date.strftime("%Y-%m-%d %H:%M:%S")

        # Simulate other fields
        sender_name = generate_chinese_name()  # Random Chinese sender name
        receiver_name = generate_chinese_name()  # Random Chinese receiver name
        sender_account = random.randint(100000, 999999)
        receiver_account = random.randint(100000, 999999)
        sender_sex = "男" if random.choice([True, False]) else "女"
        receiver_sex = "男" if random.choice([True, False]) else "女"
        sender_ip = fake.ipv4()
        receiver_ip = fake.ipv4()  # Adding receiver IP
        sender_os = random.choice(["Android 8.0", "iOS 10.0", "Android 7.0", "iOS 9.0"])
        receiver_os = random.choice(["Android 8.0", "iOS 10.0", "Android 7.0", "iOS 9.0"])

        # Choose a phone type based on the OS, popular phone types have heigher weight
        if "Android" in sender_os:

            sender_phonetype = random.choices(popular_android + phone_types_android, k=1)[0]
        else:

            sender_phonetype = random.choices(popular_ios + phone_types_ios, k=1)[0]

        if "Android" in receiver_os:

            receiver_phonetype = random.choices(popular_android + phone_types_android, k=1)[0]
        else:

            receiver_phonetype = random.choices(popular_ios + phone_types_ios, k=1)[0]

        sender_network = random.choice(["4G", "5G", "S2"])
        receiver_network = random.choice(["4G", "5G", "S2"])

        # Simulate GPS location with 80% of data within China
        is_in_china = random.random() < 0.8  # 80% of data within China
        if is_in_china:
            # Use random coordinates that are likely within China's provinces
            sender_long = random.uniform(73.5, 135.0)  # Longitude range within China
            sender_lat = random.uniform(3.0, 53.5)  # Latitude range within China
        else:
            # 20% of data will be overseas
            sender_long = random.uniform(-180, 180)
            sender_lat = random.uniform(-90, 90)

        receiver_gps = f"{random.uniform(3.0, 53.5):.6f},{random.uniform(73.5, 135.0):.6f}"

        msg_type = random.choice(["文本", "图片", "视频", "语音"])
        distance = f"{random.uniform(0, 100):.2f}KM"
        message = generate_chinese_message()

        # Simulate the log entry with all 20 columns, deliberately inserting nulls in 5% of records
        log_entry = [
            timestamp,
            sender_name,
            sender_account,
            sender_sex,
            sender_ip,
            sender_os,
            sender_phonetype,
            sender_network,
            f"{sender_long:.6f},{sender_lat:.6f}",  # Sender GPS
            receiver_name,
            receiver_ip,  # Receiver IP added here
            receiver_account,
            receiver_os,
            receiver_phonetype,
            receiver_network,
            receiver_gps,
            receiver_sex,
            msg_type,
            distance,
            message
        ]

        # Introduce some null or empty values randomly (5% of rows will have null or empty values)
        if random.random() < 0.05:
            log_entry[random.randint(0, len(log_entry) - 1)] = None  # Randomly set one column to None

        logs.append(log_entry)
    return logs


# Number of records to generate (1,000,000)
num_records = 1000000
log_data = generate_log_data(num_records)

# File path and delimiter for Hive compatibility (\001)
file_path = 'simulated_social_data.csv'

# Write the data to a CSV file with '\001' as the delimiter which is default for hive
with open(file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter='\001')  # Use \001 delimiter
    writer.writerows(log_data)

print(f"Data has been generated and saved to {file_path}.")
