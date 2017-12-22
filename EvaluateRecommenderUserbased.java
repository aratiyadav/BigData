package Recommender.SampleRecommender;

import java.io.File;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class EvaluateRecommenderUserbased
{

	public static void main(String[] args) throws Exception
	{
		//Loading the data
		DataModel model = new FileDataModel(new File(args[0]));
		
		//Mean absolute error and Root mean Square error
		
		RecommenderEvaluator MAEevaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		RecommenderEvaluator RMSEevaluator = new RMSRecommenderEvaluator();
		
		//Precision & Recall
		RecommenderIRStatsEvaluator IRevaluator = new GenericRecommenderIRStatsEvaluator();
		
		
		//PearsonCoefficientSimilarity
			RecommenderBuilder builderPearson = new MyRecommenderBuilderPearson();
		
			double result = MAEevaluator.evaluate(builderPearson, null, model, 0.9, 1.0);
			System.out.println("The Mean Absolute Error is :" + result);
		
			result = RMSEevaluator.evaluate(builderPearson, null, model, 0.9, 1.0);
			System.out.println("The Root Means Square Error is:" + result);
		
			IRStatistics stats = IRevaluator.evaluate(builderPearson,null, model, null, 10,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		
			System.out.println("Precision:" + stats.getPrecision());
			System.out.println("Recall:" + stats.getRecall());
			System.out.println("F1Measure:" + stats.getF1Measure());
		
		//EuclideanDistanceSimilarity
			RecommenderBuilder builderEuclidean = new MyRecommenderBuilderEuclidean();
		
			double resultEuclidean = MAEevaluator.evaluate(builderEuclidean, null, model, 0.9, 1.0);
			System.out.println("The Mean Absolute Error is :" + resultEuclidean);
		
			resultEuclidean = RMSEevaluator.evaluate(builderEuclidean, null, model, 0.9, 1.0);
			System.out.println("The Root Means Square Error is:" + resultEuclidean);
			
			IRStatistics statsEuclidean = IRevaluator.evaluate(builderEuclidean,null, model, null, 10,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		
			System.out.println("Precision:" + statsEuclidean.getPrecision());
			System.out.println("Recall:" + statsEuclidean.getRecall());
			System.out.println("F1Measure:" + statsEuclidean.getF1Measure());	
		
		
		//Log-Likelihood Similarity
				RecommenderBuilder builderLogLikelihood = new MyRecommenderBuilderLogLikelihood();
				
				double resultLogLikelihood = MAEevaluator.evaluate(builderLogLikelihood, null, model, 0.9, 1.0);
				System.out.println("The Mean Absolute Error is :" + resultLogLikelihood);
				
				resultLogLikelihood = RMSEevaluator.evaluate(builderLogLikelihood, null, model, 0.9, 1.0);
				System.out.println("The Root Means Square Error is:" + resultLogLikelihood);
				
				
				IRStatistics statsLogLikelihood = IRevaluator.evaluate(builderLogLikelihood,null, model, null, 10,
						GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
				
				System.out.println("Precision:" + statsLogLikelihood.getPrecision());
				System.out.println("Recall:" + statsLogLikelihood.getRecall());
				System.out.println("F1Measure:" + statsLogLikelihood.getF1Measure());
		
		
		
	}
	
}

//Pearson 
class MyRecommenderBuilderPearson implements RecommenderBuilder
{
	public Recommender buildRecommender(DataModel model) throws TasteException
	{
		//PearSon's Coefficient Similarity
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(16, similarity, model);
	
		return new GenericUserBasedRecommender(model, neighborhood, similarity);
		
	}
}

//Euclidean
class MyRecommenderBuilderEuclidean implements RecommenderBuilder
{
	public Recommender buildRecommender(DataModel model) throws TasteException
	{
		//Euclidean Coefficient Similarity
		UserSimilarity similarity = new EuclideanDistanceSimilarity(model);
		
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(16, similarity, model);
		
		return new GenericUserBasedRecommender(model, neighborhood, similarity);
		
	}
}

//LogLikelihoodSimilarity
class MyRecommenderBuilderLogLikelihood implements RecommenderBuilder
{
	public Recommender buildRecommender(DataModel model) throws TasteException
	{
		//PearSon's Coefficient Similarity
		UserSimilarity similarity = new LogLikelihoodSimilarity(model);
		
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(16, similarity, model);
		
		return new GenericUserBasedRecommender(model, neighborhood, similarity);
		
	}
}
