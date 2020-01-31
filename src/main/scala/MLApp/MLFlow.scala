package MLApp
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

class mlflow {

  def LogisticR(df: DataFrame) : Unit = {

    val featureCols = Array("gender","age","avg_glucose_level","bmi","ever_married","heart_disease","hypertension")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df1 = assembler.transform(df)
    val Array(trainingData,testingData) = df1.randomSplit(Array(0.8,0.2))
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val model = lr.fit(trainingData)
    val predictions = model.transform(testingData)
    val objectiveHistory = model.summary.objectiveHistory
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
    println("Observed Error : "+objectiveHistory.foreach(print))

  }
}
