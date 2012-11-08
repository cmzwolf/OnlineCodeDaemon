package net.ivoa.oc.daemon.jobProcessor;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import net.ivoa.pdr.business.GlobalTechConfigBusiness;
import net.ivoa.pdr.business.JobBusiness;
import net.ivoa.pdr.business.OutputsBusiness;
import net.ivoa.pdr.commons.IOFile;

/**
 * @author Carlo Maria Zwolf
 * Observatoire de Paris
 * LERMA
 */

public class ResultsProcessor {
	private static final ResultsProcessor instance = new ResultsProcessor();

	public static ResultsProcessor getInstance() {
		return instance;
	}

	private ResultsProcessor() {
	}

	public void processResults() throws SQLException, ClassNotFoundException {
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
			List<IOFile> outputs) throws SQLException, ClassNotFoundException {
		for (IOFile currentFile : outputs) {

			String servletContainer = GlobalTechConfigBusiness.getInstance()
					.getServletContainer();
			
			String url = servletContainer + "output/" + idConfiguration
					+ "." + currentFile.getFikeExtension();
			JobBusiness.getInstance().insertResults(idConfiguration, url);
		}
		JobBusiness.getInstance().markJobAsFinished(idConfiguration);
	}

	private boolean isJobFinished(Integer idConfiguration, List<IOFile> outputs) {
		boolean toReturn = true;
		for (IOFile currentFile : outputs) {
			File file = new File(currentFile.getFileDirectory() + "/"
					+ idConfiguration + "." + currentFile.getFikeExtension());
			toReturn = toReturn && (file.exists());
		}
		return toReturn;
	}
}
