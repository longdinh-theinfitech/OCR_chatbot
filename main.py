import boto3
import os
import requests, csv, json
import fitz
import re
import datetime
import psycopg2

from PIL import Image
from io import StringIO, BytesIO
from psycopg2.extras import RealDictCursor
from psycopg2 import extras
from datetime import date


slack_bot_token = os.environ["slack_bot_token"]
dify_api_key = os.environ["dify_token"]
s3_bucket_name = os.environ["bucket_name"]
drive_api_key = os.environ["drive_token"]

def extract_event_details(event):
    body = json.loads(event['body'])
    # body = event['body']
    event_data = body.get('event', {})
    try:
        message = event_data["text"].split(">")[-1] if "text" in event_data else ""
        text = message.split(".")
        function = text[0].strip()
        msg_command = text[1].strip()
        
        file_list = []    
        if "files" in event_data.keys():
            for file in event_data["files"]:
                file_list.append(file["id"])

        result = {
            "channel": event_data.get('channel'),
            "text": msg_command,
            "function": function,
            "files": file_list,
            "thread_ts": event_data.get("ts")
        }
        return result
    
    except:
        message = event_data["text"].split(">")[-1].strip()
        result = {
            "channel": event_data.get('channel'),
            "text": message,
            "function": "0",
            "thread_ts": event_data.get("ts")
        }
        return result


def upload_file_to_s3(value):
    file_info = requests.get(
        f'https://slack.com/api/files.info?file={value}',
        headers={'Authorization': f'Bearer {slack_bot_token}'}
    ).json()
    file_name = file_info['file']['name']
    file_url = file_info['file']['url_private']
    file_response = requests.get(
        file_url,
        headers={'Authorization': f'Bearer {slack_bot_token}'},
        stream=True
    )

    img = file_response.content
    s3 = boto3.resource("s3")
    s3.Bucket(s3_bucket_name).put_object(Key=file_name, Body=img)

    return file_name


def retrieve_from_drive(folder_id):
    s3 = boto3.resource("s3")
    
    try:
        url = f"https://www.googleapis.com/drive/v3/files"
        params = {
            "q": f"'{folder_id}' in parents and trashed = false",
            "key": drive_api_key,
            "fields": "files(id, name, mimeType)"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        files = response.json().get('files', [])

        if not files:
            return {"statusCode": 200, "body": "No files found in the folder."}

        retrieved_files = []
        for file in files:
            file_id = file['id']
            file_name = file['name']

            download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&key={drive_api_key}"
            download_response = requests.get(download_url, stream=True)

            file_stream = BytesIO(download_response.content)
            res = s3.Bucket(s3_bucket_name).put_object(Key=file_name, Body=file_stream)
            retrieved_files.append(file_name)

        return retrieved_files
    except Exception as e:
        return e


def convert_pdf_to_img(file_name):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_name)
    pdf_data = response['Body'].read()

    pdf_file = fitz.open(stream=BytesIO(pdf_data), filetype="pdf")
    images = []

    for page_index in range(len(pdf_file)):
        page = pdf_file.load_page(page_index)
        image_list = page.get_images(full=True)

        for image_index, img in enumerate(image_list, start=1):
            xref = img[0]

            base_image = pdf_file.extract_image(xref)
            image_bytes = base_image["image"]

            image_ext = base_image["ext"]

            image = Image.open(BytesIO(image_bytes))
            images.append(image)

    total_width = max(img.width for img in images)
    total_height = sum(img.height for img in images)

    new_image = Image.new('RGB', (total_width, total_height))
    current_height = 0
    for img in images:
        new_image.paste(img, (0, current_height))
        current_height += img.height

    img_byte_arr = BytesIO()
    new_image.save(img_byte_arr, format='JPEG')
    img_byte_arr.seek(0)

    object_key = file_name.replace(".pdf", str(datetime.datetime.now()) + ".png").replace(" ", "_")
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=object_key,
        Body=img_byte_arr,
        ContentType='image/jpeg'
    )
    return object_key


def upload_file_to_dify(file_name):
    api_url = "https://api.dify.ai/v1/files/upload"
    headers = {
        "Authorization": f'Bearer {dify_api_key}'
    }

    s3_client = boto3.client('s3')

    try:
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_name)
        file_content = response['Body'].read()

        files = {
            'file': (file_name, file_content, 'image/png')
        }
        data = {
            'user': "abc-123"
        }

        upload_response = requests.post(api_url, headers=headers, files=files, data=data)
        upload_response.raise_for_status()
        upload_file_id = upload_response.json().get("id")

        return upload_file_id

    except Exception as e:
        print(f"Error: {str(e)}")
        return ""


def dify_handler(file_id, tag):
    headers = {
        'Authorization': f'Bearer {dify_api_key}',
    }

    if file_id == "":
        return requests.post(
                "https://api.dify.ai/v1/chat-messages",
                headers=headers,
                json={
                    "inputs": {},
                    "query": tag,
                    "conversation_id": "",
                    "user": "abc-123"
                }
            )

    headers["Content-Type"] = "application/json"
    data = {
        "files": [
            {
                "type": "image",
                "transfer_method": "local_file",
                "url": "",
                "upload_file_id": file_id
            }
        ],
        "inputs": {
            "files": None,
            "tags": tag
        },
        "query": "extract information",
        "conversation_id": "",
        "user": "abc-123"
    }
    return requests.post("https://api.dify.ai/v1/chat-messages", headers=headers, json=data)



def post_message_to_thread(channel, text, thread_ts):
    url = 'https://slack.com/api/chat.postMessage'
    headers = {
        'Authorization': f'Bearer {slack_bot_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "channel": channel,
        "text": text,
        "thread_ts": thread_ts
    }
    
    request_data = json.dumps(data).encode('utf-8')
    response = requests.post(url, data=request_data, headers=headers)
    return response.json()


def post_csv_to_thread(channel, csv_content, thread_ts):
    headers = {
        "Authorization": f'Bearer {slack_bot_token}',
        "Content-Type": "application/json",
    }

    temp_time = str(datetime.datetime.now()).replace(" ", "_")
    size = len(csv_content.encode('utf-8'))
    upload_url_res = requests.get(
        f'https://slack.com/api/files.getUploadURLExternal?filename={temp_time}.csv&length={size}&pretty=1',
        headers = headers
    )
    upload_url = upload_url_res.json()["upload_url"]
    upload_id = upload_url_res.json()["file_id"]
    
    upload_handle = requests.post(
            upload_url,
            data = csv_content.encode('utf-8'),
            headers = {
                "Authorization": f'Bearer {slack_bot_token}',
                "Content-Type": "application/octet-stream",
            }
        )
        
    upload_complete = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers=headers,
        json={
            "files": [
                {"id": upload_id, "title": "card visit"}
            ],
            "channel_id": channel,
            "thread_ts": thread_ts
        }
    )

    return upload_complete.json()


def has_slack_retry_header(event):
    headers = event.get('headers', {})
    if 'x-slack-retry-num' in headers:
        return True
    return False


def json_serializer(rows):
    for row in rows:
        for key, value in row.items():
            if isinstance(value, date):
                row[key] = value.isoformat()  # Convert date to "YYYY-MM-DD"
    return rows


def query_db(start_day:str, end_day: str):
    conn_params = {
        "database": os.eviron["db_name"]
        "user": os.environ['db_username'],
        "password": os.environ['db_password'],
        "host": os.environ["db_host"],
        "port": "5432"
    }
    
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = "select * from Customers where create_at between %s and %s;"
                cursor.execute(query, (start_day, end_day))
                
                rows = cursor.fetchall()
                serialized_rows = json_serializer(rows)
                return serialized_rows
    
    except Exception as e:
        return []


def insert_db(json_data: list):
    try:
        host = os.environ["db_host"]
        database = os.environ['db_name']
        username = os.environ['db_username']
        password = os.environ['db_password']
        port = 5432
        
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password,
            port=port
        )
        cursor = conn.cursor()        
        columns = ', '.join([f'"{key}"' for key in json_data[0].keys()])
        query = f"""INSERT INTO Customers ({columns}) 
                    VALUES %s 
                    ON CONFLICT ("person_email")
                    DO UPDATE SET
                        "person_name(Kanji)" = EXCLUDED."person_name(Kanji)",
                        "person_name(Romaji)" = EXCLUDED."person_name(Romaji)",
                        person_firstname = EXCLUDED.person_firstname,
                        person_lastname = EXCLUDED.person_lastname,
                        person_phone = EXCLUDED.person_phone,
                        person_job = EXCLUDED.person_job,
                        organization_name = EXCLUDED.organization_name,
                        organization_homepage = EXCLUDED.organization_homepage,
                        organization_address = EXCLUDED.organization_address,
                        image_url = EXCLUDED.image_url,
                        tag_label = EXCLUDED.tag_label
                """
        values = [
            tuple(item[col] for col in json_data[0].keys()) for item in json_data
        ]
        print(values)
        extras.execute_values(cursor, query, values)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }


def csv_converter(json_data: list, channel, thread_ts):
    if len(json_data) > 0:
        output = StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=json_data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(json_data)
        csv_content = output.getvalue()
        output.close()
        return post_csv_to_thread(channel, csv_content, thread_ts)
    
    return post_message_to_thread(channel, "No data retrieved", thread_ts)


def db_export(json_data: list, channel, thread_ts):
    json_mapping = []
    for data in json_data:
        json_mapping.append({
            "Person - Name": data["person_name(Kanji)"],
            "Person - First name": data["person_firstname"],
            "Person - Last name": data["person_lastname"],
            "Person - Phone": data["person_phone"],
            "Person - Email": data["person_email"],
            "Person - Job Title (Custom field)": data["person_job"],
            "Person - Labels": data["tag_label"],
            "Organization - Name": data["organization_name"],
            "Organization - Website": data["organization_homepage"],
            "Organization - Address": data["organization_address"],
            "Deal - Title": "なし",
            "Deal - Value": "",
            "Deal - Currency of Value": "JPY",
            "Activity - Subject": "交流会のお礼メール",
            "Activity - Due date": "2024-12-12",
            "Note - Content": ""
        })
    if len(json_mapping) > 0:
        output = StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=json_mapping[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(json_mapping)
        csv_content = output.getvalue()
        output.close()
        return post_csv_to_thread(channel, csv_content, thread_ts)
    
    return post_message_to_thread(channel, "No data retrieved", thread_ts)


def result_handler(file_name: str, text, channel, thread_ts):
    base_url = "https://theinfitech-slack-storage.s3.us-east-1.amazonaws.com/"

    image_name = ""
    if "pdf" in file_name:
        image_name = image_name + convert_pdf_to_img(file_name)
    else:
        image_name = image_name + file_name
    file_id = upload_file_to_dify(image_name)
    dify_response = dify_handler(file_id, text).json()

    try:
        item = json.loads(dify_response["answer"])
        s3_client = boto3.client('s3')

        slack_item = json.loads(item[0])
        db_item = json.loads(item[1])

        response = s3_client.get_object(Bucket=s3_bucket_name, Key=image_name)
        new_image_name = (
            db_item["person_email"] if db_item["person_email"] is not None else "No email"
        ) + "_" + str(datetime.datetime.now()).replace(" ", "_") + ".png"          

        copy_res = s3_client.copy_object(
            Bucket=s3_bucket_name,
            Key=new_image_name,
            CopySource=f"{s3_bucket_name}/{image_name}"
        )
        
        slack_item["Image Link"] = base_url + new_image_name
        db_item["image_url"] = base_url + new_image_name

        return slack_item, db_item

    except Exception as e:
        slack_response = post_message_to_thread(channel, dify_response["message"] , thread_ts)
        return None, None


def lambda_handler(event, context):
    if has_slack_retry_header(event):
        return {
            'statusCode': 200
        }
    
    result = extract_event_details(event)
    if result["function"] == "1":
        json_data = []
        rows = []
        base_url = "https://theinfitech-slack-storage.s3.us-east-1.amazonaws.com/"

        for value in result["files"]:
            file_name = upload_file_to_s3(value)
            slack_item, db_item = result_handler(file_name, result["text"], result["channel"], result["thread_ts"])
            json_data.append(slack_item)
            rows.append(db_item)
        res = csv_converter(json_data, result["channel"], result["thread_ts"])
        insert_db(rows)
        
    elif result["function"] == "2":
        times_query = result["text"].split(",")
        start_day = times_query[0].strip()
        end_day = times_query[1].strip()
        json_data = query_db(start_day, end_day)
        res = db_export(json_data, result["channel"], result["thread_ts"])

    elif result["function"] == "3":
        json_data = []
        rows = []
        command = result["text"].split(",")
        folder_id = command[0].strip()
        tag_label = ""
        if len(command) > 1:
            tag_label += command[1].strip()
        file_list = retrieve_from_drive(folder_id)

        for file in file_list:
            slack_item, db_item = result_handler(file, tag_label, result["channel"], result["thread_ts"])
            if slack_item != None and db_item != None:
                json_data.append(slack_item)
                rows.append(db_item)
        res = csv_converter(json_data, result["channel"], result["thread_ts"])
        insert_db(rows)

    else:
        dify_response = dify_handler("", result["text"])
        answer = dify_response.json()["answer"]
        slack_response = post_message_to_thread(result["channel"], answer, result["thread_ts"])
