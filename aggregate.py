"""Aggregate module"""

import json
from datetime import timedelta

from db import get_collection
from validate_data import InputData


def convert_input_data(data: str):
    """Валидация данных"""

    try:
        data = json.loads(data)
        return InputData(**data)  # type: ignore
    except ValueError:
        return None
    except TypeError:
        return None


def generate_time_ranges(start_date, end_date, freq):
    """Генерация временных меток для указанного диапазона."""
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date)
        if freq == "hour":
            current_date += timedelta(hours=1)
        elif freq == "day":
            current_date += timedelta(days=1)
        elif freq == "week":
            current_date += timedelta(weeks=1)
        elif freq == "month":
            current_date = (current_date.replace(day=28) + timedelta(days=4)).replace(
                day=1
            )
    return date_range


def create_documents(date_range, freq):
    """Создание документов для всех временных дат в диапазоне."""
    if freq == "hour":
        return [
            {
                "_id": {
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "hour": date.hour,
                },
                "total_value": 0,
                "label": date.isoformat(),
            }
            for date in date_range
        ]
    elif freq == "day":
        return [
            {
                "_id": {
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                },
                "total_value": 0,
                "label": date.isoformat(),
            }
            for date in date_range
        ]
    elif freq == "week":
        return [
            {
                "_id": {
                    "year": date.year,
                    "month": date.month,
                    "week": date.isocalendar()[1],
                },
                "total_value": 0,
                "label": date.isoformat(),
            }
            for date in date_range
        ]
    elif freq == "month":
        return [
            {
                "_id": {
                    "year": date.year,
                    "month": date.month,
                },
                "total_value": 0,
                "label": date.isoformat(),
            }
            for date in date_range
        ]


def create_pipeline(start_date, end_date, freq):
    """Создание агрегации для указанного диапазона."""
    group_id = {
        "year": {"$year": "$dt"},
        "month": {"$month": "$dt"},
    }
    if freq == "hour":
        group_id["day"] = {"$dayOfMonth": "$dt"}
        group_id["hour"] = {"$hour": "$dt"}
    elif freq == "day":
        group_id["day"] = {"$dayOfMonth": "$dt"}
    elif freq == "week":
        group_id["week"] = {"$isoWeek": "$dt"}

    return [
        {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
        {
            "$group": {
                "_id": group_id,
                "total_value": {"$sum": {"$ifNull": ["$value", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]


def aggregate(collection, start_date, end_date, freq):
    """Агрегация данных из БД по указанному диапазону с значением по умолчанию 0, если данные отсутствуют."""
    date_range = generate_time_ranges(start_date, end_date, freq)
    all_docs = create_documents(date_range, freq)
    pipeline = create_pipeline(start_date, end_date, freq)

    result = list(collection.aggregate(pipeline))

    result_dict = {tuple(item["_id"].values()): item["total_value"] for item in result}

    final_dataset = []
    final_labels = []
    for doc in all_docs:  # type: ignore
        key = tuple(doc["_id"].values())
        doc["total_value"] = result_dict.get(key, 0)
        final_dataset.append(doc["total_value"])
        final_labels.append(doc["label"])

    return {"dataset": final_dataset, "labels": final_labels}


def get_result(data: str):
    """Получить итоговые данные"""
    collection = get_collection()
    valid_data = convert_input_data(data=data)

    if not (valid_data):
        return "Error! Invalid input data"

    aggregate_group = valid_data.group_type
    start_date = valid_data.dt_from
    end_date = valid_data.dt_upto

    match aggregate_group:
        case "hour":
            aggregation_result = aggregate(collection, start_date, end_date, "hour")
        case "day":
            aggregation_result = aggregate(collection, start_date, end_date, "day")
        case "week":
            aggregation_result = aggregate(collection, start_date, end_date, "week")
        case "month":
            aggregation_result = aggregate(collection, start_date, end_date, "month")

    return aggregation_result
