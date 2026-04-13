import psycopg2

from db_driver import PostgresDriver


def main() -> None:
    driver = PostgresDriver()
    try:
        driver.create_tables()

        seed_users = [
            ("Анна", 27),
            ("Борис", 35),
            ("Светлана", 22),
        ]
        user_ids: dict[str, int] = {}

        for name, age in seed_users:
            existing_user = driver.get_user_by_name(name)
            if existing_user:
                user_ids[name] = int(existing_user["id"])
            else:
                user_ids[name] = driver.add_user(name, age)

        seed_orders = [
            ("Анна", 1250.00),
            ("Борис", 430.50),
        ]
        for user_name, amount in seed_orders:
            user_id = user_ids[user_name]
            if not driver.order_exists(user_id, amount):
                driver.add_order(user_id, amount)

        totals = driver.get_user_totals()
        print("Сумма заказов по пользователям:")
        for item in totals:
            print(f"{item['name']} — {float(item['total_amount']):.2f}")
        print(f"Пользователей в выборке: {len(user_ids)}")
    except psycopg2.Error as error:
        print(f"Ошибка PostgreSQL: {error.pgerror or str(error)}")
    except Exception as error:
        print(f"Ошибка приложения: {error}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
