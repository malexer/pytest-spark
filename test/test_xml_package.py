import pytest

try:
    from pyspark.sql import SparkSession
except ImportError:
    SPARK1 = True
else:
    SPARK1 = False


xml_str = u"""
<?xml version="1.0"?>
<items>
   <item id="1">
      <title>one</title>
      <int>1</int>
      <roman>I</roman>
   </item>
   <item id="4">
      <title>four</title>
      <int>4</int>
      <roman>IV</roman>
   </item>
</items>
"""


@pytest.mark.skipif(SPARK1, reason="requires Spark 2.x")
def test_read_xml(spark_session, tmp_path):
    xml = tmp_path / 'pytest_spark_test_items.xml'
    xml.write_text(xml_str)
    xml_filename = str(xml)

    df = spark_session \
        .read.format('xml') \
        .options(rowTag='item') \
        .load(xml_filename)

    data = df.select("title", "roman").where(df._id == 4).collect()
    data = [r.asDict() for r in data]

    assert data == [dict(title=u'four', roman=u'IV')]
