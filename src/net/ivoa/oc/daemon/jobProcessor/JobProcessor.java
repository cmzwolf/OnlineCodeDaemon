package net.ivoa.oc.daemon.jobProcessor;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import net.ivoa.pdr.business.InputFileBusiness;
import net.ivoa.pdr.business.JobBusiness;
import net.ivoa.pdr.business.ParametersBusiness;
import net.ivoa.pdr.commons.IOFile;

/**
 * @author Carlo Maria Zwolf Observatoire de Paris LERMA
 */

public class JobProcessor {
	private static final JobProcessor instance = new JobProcessor();

	public static JobProcessor getInstance() {
		return instance;
	}

	private JobProcessor() {
	}

	public void processJobs() throws SQLException, ClassNotFoundException {
		List<Integer> idJobsToProcess = JobBusiness.getInstance()
				.getNotProcessedJobs();

		List<IOFile> inputFilesPatterns = InputFileBusiness.getInstance()
				.getPatternInputFile();

		for (Integer currentId : idJobsToProcess) {
			JobProcessor.getInstance().processSingleJob(currentId,
					inputFilesPatterns);
		}
	}

	private void processSingleJob(Integer idConfiguration,
			List<IOFile> inputFilesPatterns) throws SQLException,
			ClassNotFoundException {
		try {

			Map<String, String> configurationContent = ParametersBusiness
					.getInstance().getConfigurationMap(idConfiguration);

			for (IOFile inputfilePattern : inputFilesPatterns) {

				String[] replacedValuesFileContent = JobProcessor.getInstance()
						.buildInputFileContent(idConfiguration,
								inputfilePattern.getFilePattern(),
								configurationContent);

				JobProcessor.getInstance().writeInputFile(
						replacedValuesFileContent, idConfiguration,
						inputfilePattern.getFileDirectory(),
						inputfilePattern.getFileExtension());

			}

			JobBusiness.getInstance().markJobAsProcessed(idConfiguration);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	private void writeInputFile(String[] fileContent, Integer idConfiguration,
			String inputDir, String fileExtension) throws FileNotFoundException {

		PrintWriter writer = null;

		writer = new PrintWriter(inputDir + "/" + idConfiguration.toString()
				+ "." + fileExtension);
		for (int i = 0; i < fileContent.length; i++) {
			writer.println(fileContent[i]);
		}
		writer.close();
	}

	private String[] buildInputFileContent(Integer idConfiguration,
			String patternFileContent, Map<String, String> configurationContent)
			throws SQLException, ClassNotFoundException {

		String[] fileLine = patternFileContent.split("\n");

		for (int i = 0; i < fileLine.length; i++) {
			fileLine[i] = fileLine[i].replace("$$RunId$$",
					idConfiguration.toString());
			for (Map.Entry<String, String> entry : configurationContent
					.entrySet()) {

				String toReplace = "$$"+entry.getKey()+"$$";
				fileLine[i] = fileLine[i].replace(toReplace, entry.getValue());
			}
		}
		return fileLine;
	}

}
