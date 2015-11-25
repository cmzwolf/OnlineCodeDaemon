package net.ivoa.oc.daemon.jobProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;

import net.ivoa.pdr.business.GlobalTechConfigBusiness;
import net.ivoa.pdr.business.JobBusiness;
import net.ivoa.pdr.business.OutputsBusiness;
import net.ivoa.pdr.commons.IOFile;

/**
 * @author Carlo Maria Zwolf Observatoire de Paris LERMA
 */

public class ResultsProcessor {
	private static final ResultsProcessor instance = new ResultsProcessor();

	public static ResultsProcessor getInstance() {
		return instance;
	}

	private ResultsProcessor() {
	}

	public void processResults() throws SQLException, ClassNotFoundException,
			MalformedURLException, IOException {
		List<Integer> idProcessedJobs = JobBusiness.getInstance()
				.getProcessedJobs();

		List<IOFile> outputs = OutputsBusiness.getInstance()
				.getPatternOutputFile();

		for (Integer currentConfigId : idProcessedJobs) {
			
			System.out.println("\n Processing job "+ currentConfigId);
			
			boolean isJobFinished = ResultsProcessor.getInstance()
					.isJobFinished(currentConfigId, outputs);
			
			
			System.out.println("Job is finished "+ isJobFinished);
			
			boolean hasJobError = ResultsProcessor.getInstance().hasJobError(
					currentConfigId, outputs);
			
			System.out.println("Job has Errors "+ hasJobError);
			
			// if the job is finished without error
			if (isJobFinished && !hasJobError) {
				System.out.println("Here 1");
				ResultsProcessor.getInstance().processFinishedJob(
						currentConfigId, outputs);
			}

			// if the job is finished with error
			if (isJobFinished && hasJobError) {
				System.out.println("Here 2");
				ResultsProcessor.getInstance().processFinishedJobWithErrors(
						currentConfigId, outputs);
			}

			// if the job is not finished but has error
			if (!isJobFinished && hasJobError) {
				System.out.println("Here 3");
				ResultsProcessor.getInstance().processErrorsOnJob(
						currentConfigId, outputs);
			}
		}

	}

	private void processFinishedJob(Integer idConfiguration,
			List<IOFile> outputs) throws SQLException, ClassNotFoundException,
			MalformedURLException, IOException {
		for (IOFile currentFile : outputs) {

			String servletContainer = GlobalTechConfigBusiness.getInstance()
					.getServletContainer();

			String resultName = currentFile.getResultName();

			String fileUrl = servletContainer + "output/" + idConfiguration
					+ "." + currentFile.getFileExtension();

			String result = "";

			if (currentFile.getFileExtension().endsWith(".Value")) {
				Integer totalLengtOfFileExtension = currentFile
						.getFileExtension().length();
				String parameterName = currentFile.getFileExtension()
						.substring(0, totalLengtOfFileExtension - 6);

				result = getFileContentFromUrl(fileUrl);

				resultName = parameterName;
			} else {
				result = fileUrl;
			}
			JobBusiness.getInstance().insertResults(idConfiguration, result,
					resultName);
		}
		JobBusiness.getInstance().markJobAsFinished(idConfiguration);
	}

	private void processErrorsOnJob(Integer idConfiguration,
			List<IOFile> outputs) throws ClassNotFoundException, SQLException {
		for (IOFile currentFile : outputs) {

			File errorFile = new File(currentFile.getFileDirectory() + "/"
					+ idConfiguration + ".error");

			if (errorFile.exists()) {
				String servletContainer = GlobalTechConfigBusiness
						.getInstance().getServletContainer();

				String errorFileURL = servletContainer + "output/"
						+ idConfiguration + ".error";

				String resultName = "errorFile";

				JobBusiness.getInstance().insertResults(idConfiguration,
						errorFileURL, resultName);
			}
		}
		JobBusiness.getInstance().markJobAsHavingErrors(idConfiguration);
	}

	private void processFinishedJobWithErrors(Integer idConfiguration,
			List<IOFile> outputs) throws SQLException, ClassNotFoundException,
			MalformedURLException, IOException {
		processFinishedJob(idConfiguration, outputs);
		processErrorsOnJob(idConfiguration, outputs);
	}

	private String getFileContentFromUrl(String fileUrl)
			throws MalformedURLException, IOException {
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new URL(fileUrl).openConnection()
						.getInputStream()));

		StringBuilder sb = new StringBuilder();
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			sb.append(line);
			sb.append("\n");
		}
		bufferedReader.close();
		return sb.toString();
	}

	private boolean isJobFinished(Integer idConfiguration, List<IOFile> outputs) {
		boolean toReturn = true;
		for (IOFile currentFile : outputs) {
			
			System.out.println("Fetching for file "+ currentFile.getFileDirectory() + "/"
					+ idConfiguration + "." + currentFile.getFileExtension());
			
			File file = new File(currentFile.getFileDirectory() + "/"
					+ idConfiguration + "." + currentFile.getFileExtension());
			toReturn = toReturn && (file.exists());
		}
		return toReturn;
	}

	private boolean hasJobError(Integer idConfiguration, List<IOFile> outputs) {
		boolean toReturn = false;
		for (IOFile currentFile : outputs) {
			File file = new File(currentFile.getFileDirectory() + "/"
					+ idConfiguration + ".error");
			toReturn = toReturn || (file.exists());
		}
		return toReturn;
	}

}
