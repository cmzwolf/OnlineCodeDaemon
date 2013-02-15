package net.ivoa.oc.daemon.jobProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;

import javax.print.attribute.standard.MediaSize.Other;

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
				.getPatternInputFile();

		for (Integer currentConfigId : idProcessedJobs) {
			boolean isJobFinished = ResultsProcessor.getInstance()
					.isJobFinished(currentConfigId, outputs);
			if (isJobFinished) {
				ResultsProcessor.getInstance().processFinishedJob(
						currentConfigId, outputs);
			} else {
				System.out.println("no job finished");
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
			File file = new File(currentFile.getFileDirectory() + "/"
					+ idConfiguration + "." + currentFile.getFileExtension());
			toReturn = toReturn && (file.exists());
		}
		return toReturn;
	}

}
