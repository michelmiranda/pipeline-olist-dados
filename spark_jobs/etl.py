from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark

def process_payment_type(spark, input_data, output_data):
    payment_type_data = input_data + 'olist_order_payments_dataset.csv'
    df = spark.read.csv(payment_type_data, header=True, inferSchema=True)

    payment_type_table = df.select('payment_type').distinct()

    payment_type_table.coalesce(1).write.mode('overwrite').parquet(output_data + 'payment_type')

def process_category_product(spark, input_data, output_data):
    category_product_data = input_data + 'product_category_name_translation.csv'
    df = spark.read.csv(category_product_data, header=True, inferSchema=True)

    category_product_table = df.select('product_category_name','product_category_name_english').distinct()

    category_product_table.coalesce(1).write.mode('overwrite').parquet(output_data + 'category_product')


def check_results_payment_type(spark, output_data):
    df = spark.read.parquet(output_data+'payment_type')
    df.select('payment_type')

def check_results_category_product(spark, output_data):
    df = spark.read.parquet(output_data+'category_product')
    df.select('product_category_name','product_category_name_english')


def main():

    spark = create_spark_session()

    input_data = '../../../datasets/'
    output_data = '../../../output/'

    process_payment_type(spark,input_data,output_data)
    check_results_payment_type(spark, output_data)

    process_category_product(spark, input_data, output_data)
    check_results_category_product(spark, output_data)

if __name__ == "__main__":
    main()
