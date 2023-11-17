import pymongo
from pymongo import MongoClient
from flask import jsonify
import sys
import os
from datetime import datetime
from bson.objectid import ObjectId


from pymongo import MongoClient
import classes.task.manager as task_manager

tm = task_manager.TaskManager()

def generate_zero_response():
    zero_response = {
        "balances": {
            'labels': [],
            'datasets': []
        },
        "remark": {},
        "trades_data": {
            "total_trades": 0,
            "total_win": 0,
            "total_loss": 0,
            "risk_average": 0,
            "risk_percentage": 0,
            "trades_per_day_average": 0,
            "ratio_win_loss": 0,
            "total_win_percentage": 0,
            "win_buy_percentage": 0,
            "win_sell_percentage": 0,
            "instruments_traded": 0,
            "avg_lots": 0,
            "time_open_trade": 0,
            "commission_trade": 0
        },
        "max_drawdown_percent": 0,
        "balance_info": {
            "min_balance": 0,
            "min_balance_day": "",
            "days_to_recover_initial_balance": 0,
            "days_since_min_balance": 0
        },
        "symbols_data": {
            "total_instruments_traded": []
        }
    }
    return zero_response


def is_valid_objectid(value):
    try:
        ObjectId(value)
        return True
    except:
        return False

def get_metadata_by_user(id):
    try:
        if is_valid_objectid(id):
            trading_account = tm.trading_accounts.db.collection.find_one({"_id": ObjectId(id)})
        else:
            trading_account = tm.trading_accounts.db.collection.find_one({"mt_user_id": id})
            mt_user_id = str(trading_account['mt_user_id'])
        
        if trading_account is None:
            return {"error": "No se encontró la cuenta de trading"}, 404

        print(trading_account['mt_user_id'])
        balances = list(tm.balances.db.collection.find_one(
            {"mt_user_id": trading_account['mt_user_id']}).sort('date_year_month_day'))
        
        if not balances:
            return generate_zero_response()
        
        print('obteniendo remarks')
        remark = tm.remarks.db.collection.find_one({"key": trading_account['remarks']})
        
        if remark is None:
            return
        
        total_trades = tm.trades.db.collection.count_documents(
            {"mt_user_id": trading_account['mt_user_id']})
        print(total_trades)
        
        if total_trades == 0:
            return generate_zero_response()

        else:
            pipeline = [
                {
                    "$match": {
                        "mt_user_id": trading_account['mt_user_id'],
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_trades": {"$sum": 1},
                        "win_count": {
                            "$sum": {
                                "$cond": [{"$gt": ["$profit", 0]}, 1, 0]
                            }
                        },
                        "loss_count": {
                            "$sum": {
                                "$cond": [{"$lt": ["$profit", 0]}, 1, 0]
                            }
                        },
                        "total_loss_value": {
                            "$sum": {
                                "$cond": [{"$lt": ["$profit", 0]}, "$profit", 0]
                            }
                        },
                        "win_count_buy": {
                            "$sum": {
                                "$cond": [{"$and": [{"$eq": ["$direction", "buy"]}, {"$gt": ["$profit", 0]}]}, 1, 0]
                            }
                        },
                        "win_count_sell": {
                            "$sum": {
                                "$cond": [{"$and": [{"$eq": ["$direction", "sell"]}, {"$gt": ["$profit", 0]}]}, 1, 0]
                            }
                        }
                    }
                }
            ]

            aggregation = list(Trades.objects.aggregate(*pipeline))

            if len(aggregation):
                aggregation = aggregation[0]
            else:
                aggregation = None

            if aggregation:

                win_count = round(aggregation['win_count'], 2)
                print("win_count.................", win_count)

                loss_count = round(aggregation['loss_count'], 2)
                print("loss_count.................", loss_count)

                total_buy = round(aggregation['win_count_buy'], 2)
                print("total_buy.................", total_buy)

                total_sell = round(aggregation['win_count_sell'], 2)
                print("total_sell.................", total_sell)

                total_trades = round(aggregation['total_trades'], 2)
                print("total_trades.................", total_trades)

                total_loss_value = round(aggregation['total_loss_value'], 2)
                print("total_loss_value.................", total_loss_value)

                risk_average = abs(round(
                    total_loss_value / loss_count, 2) if total_trades != 0 else 0)
                risk_percentage = abs(round(
                    risk_average / remark['amount'] * 100, 2))

                ratio_win_loss = abs(round(
                    aggregation['win_count'] / aggregation['loss_count'], 2) if aggregation['loss_count'] != 0 else 0)
                total_win_percentage = round(
                    aggregation['win_count'] / total_trades * 100, 2) if total_trades != 0 else 0
                win_buy_percentage = round(
                    aggregation['win_count_buy'] / win_count * 100, 2) if win_count != 0 else 0
                win_sell_percentage = round(
                    aggregation['win_count_sell'] / win_count * 100, 2) if win_count != 0 else 0
            else:
                win_count = 0
                loss_count = 0
                risk_average = 0
                risk_percentage = 0
                ratio_win_loss = 0
                total_win_percentage = 0
                win_buy_percentage = 0
                win_sell_percentage = 0
            print(aggregation)
            pipeline = [
                {
                    "$match": {
                        "mt_user_id": trading_account['mt_user_id']
                    }
                },
                {
                    "$group": {
                        "_id": "$open_time",
                        "trades_count": {"$sum": 1}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "average_trades_per_day": {"$avg": "$trades_count"}
                    }
                }
            ]

            trades_per_day_average = list(Trades.objects.aggregate(*pipeline))

            print(trades_per_day_average)
            if len(trades_per_day_average):
                trades_per_day_average = round(
                    trades_per_day_average[0]['average_trades_per_day'], 2)
            else:
                trades_per_day_average = 0
            print(f'len(balances): {len(balances)}')

            total_instruments_traded = Trades.objects.aggregate([
                {
                    "$match": {
                        "mt_user_id": trading_account['mt_user_id'],
                    }
                },
                {
                    "$group": {
                        "_id": "$symbol",
                        "count": {"$sum": 1},
                        "avg_lots": {"$avg": "$lots"}
                    }
                }
            ])

            total_instruments_traded = [{
                "symbol": symbol_info["_id"],
                "count": symbol_info["count"],
                "percent": round(symbol_info["count"] / total_trades * 100, 2),
                "avg_lots": round(symbol_info["avg_lots"], 2)


            } for symbol_info in list(total_instruments_traded)]

            if len(total_instruments_traded):
                instruments_traded = total_instruments_traded[0]["percent"]
                avg_lots = total_instruments_traded[0]["avg_lots"]

                total_instruments_traded.sort(
                    key=lambda x: x["count"], reverse=True)
                top_4_instruments = total_instruments_traded[:4]

            else:
                instruments_traded = 0
                avg_lots = 0
                top_4_instruments = []

            if top_4_instruments is not None:
                for instrument in top_4_instruments:
                    instrument["percent"] = round(
                        instrument["count"] / total_trades * 100, 2)

                symbol_to_percent_dict = {
                    instrument["symbol"]: instrument["percent"] for instrument in total_instruments_traded}
            else:
                symbol_to_percent_dict = {
                    "symbol": 0,
                    "percent": 0,
                    "avg_lots": 0,
                    "count": 0

                }

            pipeline_avg_opentime_per_trade = [
                {
                    "$match": {
                        "mt_user_id": trading_account['mt_user_id']
                    }
                },
                {
                    "$group": {
                        "_id": "$mt_user_id",
                        "average_duration": {"$avg": "$duration"},
                        "average_commission_trade": {"$avg": "$commission"}
                    }
                }
            ]

            average_open_time_commission_trade = list(
                Trades.objects.aggregate(*pipeline_avg_opentime_per_trade))

            if average_open_time_commission_trade and average_open_time_commission_trade[0]['_id'] is not None:
                average_duration = round(
                    average_open_time_commission_trade[0]['average_duration'], 2)
                average_duration = milliseconds_to_time(average_duration)
                average_commission_trade = round(
                    average_open_time_commission_trade[0]['average_commission_trade'], 2)

            else:
                average_duration = 0
                average_commission_trade = 0

            max_drawdown_percent = trading_account.min_balance / remark.amount * 100

            min_balance = remark.amount
            min_balance_day = None
            days_to_recover_initial_balance = 0
            days_since_min_balance = 0
            i = 0
            low_day_index = 0
            for balance_data in balances:
                i += 1
                if balance_data.balance < min_balance:
                    low_day_index = i
                    min_balance = balance_data.balance
                    min_balance_day = datetime.strptime(
                        balance_data.date_year_month_day, '%Y-%m-%d')
                    continue

            if min_balance < remark.amount:
                balances = list(balances)
                # [1,2,3,4,5,6,7,8 ]   => [6,7,8]
                for balance_data in balances[low_day_index:]:
                    days_since_min_balance += 1
                    if balance_data.balance >= remark.amount:
                        days_to_recover_initial_balance = (datetime.strptime(
                            balance_data.date_year_month_day, '%Y-%m-%d') - min_balance_day).days

                        break

            else:
                days_to_recover_initial_balance = 0
                days_since_min_balance = 0

            labels = [
                balance_data.date_year_month_day for balance_data in balances]
            datasets = [
                {
                    "label": "Balance",
                    "backgroundColor": "rgba(75,192,192,0.2)",
                    "borderColor": "rgba(75,192,192,1)",
                    "borderWidth": 1,
                    "hoverBackgroundColor": "rgba(75,192,192,0.4)",
                    "hoverBorderColor": "rgba(75,192,192,1)",
                    "data": [balance_data.balance for balance_data in balances]
                }
            ]
            response = {
                'labels': labels,
                'datasets': datasets
            }

            labels_instruments = list(symbol_to_percent_dict.keys())
            data_instruments = list(symbol_to_percent_dict.values())

            transformed_data = {
                'labels': labels_instruments,
                'datasets': [
                    {
                        'label': 'Instruments',
                        'data': data_instruments,
                        'backgroundColor': 'rgba(75, 192, 192, 0.6)',
                        'borderColor': 'rgba(75, 192, 192, 1)',
                        'borderWidth': 1
                    }
                ]
            }

            print(transformed_data)

            result = {
                "balances": response,
                "instrument_data": transformed_data,
                "remark": remark.to_json() if remark else {},
                "trades_data": {
                    "total_trades": total_trades,
                    "total_win": win_count,
                    "total_loss": loss_count,
                    "risk_average": risk_average,
                    "risk_percentage": risk_percentage,
                    "trades_per_day_average": trades_per_day_average,
                    "ratio_win_loss": ratio_win_loss,
                    "total_win_percentage": total_win_percentage,
                    "win_buy_percentage": win_buy_percentage,
                    "win_sell_percentage": win_sell_percentage,
                    "instruments_traded": instruments_traded,
                    "avg_lots": avg_lots,
                    "time_open_trade": average_duration,
                    "commission_trade": average_commission_trade,

                },
                "max_drawdown_percent": max_drawdown_percent,
                "balance_info": {
                    "min_balance": min_balance if min_balance else "",
                    "min_balance_day": min_balance_day.strftime('%Y-%m-%d') if min_balance_day else "",
                    "days_to_recover_initial_balance": days_to_recover_initial_balance,
                    "days_since_min_balance": days_since_min_balance
                },
                "symbols_data": {
                    "total_instruments_traded": total_instruments_traded

                }


            }

            return result

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('--', exc_type, fname, exc_tb.tb_lineno)
        print('--', e)
        return jsonify({"msg": "An error occurred", "error": str(e)}), 500


