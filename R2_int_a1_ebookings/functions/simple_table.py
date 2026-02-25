import json
import math


def sum_by(alist: list, fieldname: str) -> float:
    result = 0
    for item in alist:
        result += float(item[fieldname])
    return result


def group_by_count(alist: list, groupby_fieldname: str) -> int:
    result = {}
    for item in alist:
        value = item[groupby_fieldname]
        if value in result:
            result[value] += 1
        else:
            result[value] = 1
    print(f"result : {result}")
    return result


# def sanitize_strike_weight_list(strike_weight_list, sales_area_count):
#     result = []
#     for item in strike_weight_list:
#         fraction = item["spotsPercentage"] / 100
#         rounded_down = math.floor(fraction * sales_area_count)
#         result.append({"rounded_down": rounded_down, "spotsPercentage": rounded_down / sales_area_count * 100})
#         item["spotsPercentage"] = rounded_down / sales_area_count * 100
#     print(sales_area_count)
#     print(result)
#     return strike_weight_list


def sanitize_strike_weight_list(strike_weight_list, sales_area_count):
    total_percentage = sum(item["spotsPercentage"] for item in strike_weight_list)
    if total_percentage > 100:
        # Normalize percentages if total exceeds 100
        strike_weight_list = [
            {"spotsPercentage": item["spotsPercentage"] / total_percentage * 100}
            for item in strike_weight_list
        ]

    total_rounded_value = 0
    result = []
    for item in strike_weight_list:
        percentage = item["spotsPercentage"]
        if percentage == 0:
            rounded_value = 0
        else:
            fraction = percentage / 100
            rounded_value = max(
                round(fraction * sales_area_count), 0
            )  # Ensure the fraction value is at least 1
            total_rounded_value += rounded_value
        result.append(
            {
                "actual_spot_value": percentage / 100 * sales_area_count,
                "rounded_spot_value": rounded_value,
                "percent_of_count": rounded_value / sales_area_count * 100,
            }
        )
        item["spotsPercentage"] = rounded_value / sales_area_count * 100
    print(f"Result before total_rounded_value check: {result}")
    excess = sales_area_count - total_rounded_value
    if excess < 0:
        result[-1]["rounded_spot_value"] -= abs(excess)
        result[-1]["percent_of_count"] = (
            result[-1]["rounded_spot_value"] / sales_area_count * 100
        )
        print(f"Result after total_rounded_value check: {result}")
        strike_weight_list[-1]["spotsPercentage"] = result[-1]["percent_of_count"]

    if excess > 0:
        result[-1]["rounded_spot_value"] += excess
        result[-1]["percent_of_count"] = (
            result[-1]["rounded_spot_value"] / sales_area_count * 100
        )
        print(f"Result after total_rounded_value check: {result}")
        strike_weight_list[-1]["spotsPercentage"] = result[-1]["percent_of_count"]

    print(f"final strike weight list: {result}")
    return strike_weight_list


def sanitize_delivery_length(alist: list, length_data: list, groupby_fieldname: str):
    all_keys = group_by_count(alist, groupby_fieldname)
    all_key_list = list(all_keys.keys())
    current_length = {obj["spotLength"] for obj in length_data}
    print(f"all_key_list: {all_key_list}")
    # for item in all_key_list:
    #     # if item not in current_length:
    #     #     new_object = {"spotLength": item, "percentage": 0}
    #     #     length_data.append(new_object)
    return length_data


def group_by_sum(alist: list, groupby_fieldname: str, sum_fieldname: str) -> float:
    result = {}
    for item in alist:
        value = item[groupby_fieldname]
        if value in result:
            result[value] += float(item[sum_fieldname])
        else:
            result[value] = float(item[sum_fieldname])


def min_list(alist: list, fieldname: str) -> any:
    result = None
    for item in alist:
        value = item[fieldname]
        if result == None or result > value:
            result = value
    return result


def max_list(alist: list, fieldname: str) -> any:
    result = None
    for item in alist:
        value = item[fieldname]
        if result == None or result < value:
            result = value
    return result
