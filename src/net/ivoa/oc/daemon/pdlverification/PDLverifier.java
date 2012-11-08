package net.ivoa.oc.daemon.pdlverification;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import net.ivoa.parameter.model.Service;
import net.ivoa.parameter.model.SingleParameter;
import net.ivoa.pdl.interpreter.conditionalStatement.StatementHelperContainer;
import net.ivoa.pdl.interpreter.groupInterpreter.GroupHandlerHelper;
import net.ivoa.pdl.interpreter.groupInterpreter.GroupProcessor;
import net.ivoa.pdl.interpreter.utilities.UserMapper;
import net.ivoa.pdl.interpreter.utilities.Utilities;
import net.ivoa.pdr.business.GlobalTechConfigBusiness;
import net.ivoa.pdr.business.JobBusiness;
import net.ivoa.pdr.business.MailSenderBusiness;
import net.ivoa.pdr.business.PurgeBusiness;
import net.ivoa.pdr.commons.JobBean;

import org.apache.commons.mail.EmailException;

import visitors.GeneralParameterVisitor;
import CommonsObjects.GeneralParameter;

/**
 * @author Carlo Maria Zwolf
 * Observatoire de Paris
 * LERMA
 */

public class PDLverifier {
	private static final PDLverifier instance = new PDLverifier();

	public static PDLverifier getInstance() {
		return instance;
	}

	private PDLverifier() {
	}

	public void performPDLVerificationOnJobs() throws SQLException,
			ClassNotFoundException, EmailException {
		try {
			// get the PDL Service object from the description
			Service service = buildPDLModelObjectFromDescription();
			
			// Storing this description into the utility static field
			Utilities.getInstance().setService(service);
			
			// get the list of job that has not been processed
			List<Integer> idNotProcessedJobs = JobBusiness.getInstance()
					.getNotProcessedJobs();

			// build the map 'parameterName, SingleParameter' for getting the
			// SingleParameter instance, starting
			// from its name
			Map<String, SingleParameter> nameParamMap = new HashMap<String, SingleParameter>();
			for (SingleParameter currentParameter : service.getParameters()
					.getParameter()) {
				nameParamMap.put(currentParameter.getName(), currentParameter);
			}

			// Initializing the GeneralParameterVisitor, for defining
			// GeneralParameters objects
			GeneralParameterVisitor visitor = new GeneralParameterVisitor();

			// Creating a list of Id for storing the jobs to delete
			List<Integer> idJobsToDelete = new ArrayList<Integer>();

			// for every job of that list
			for (Integer currentId : idNotProcessedJobs) {
				// Building the job object starting from its id.
				JobBean currentJob = JobBusiness.getInstance()
						.getJobBeanFromIdJob(currentId);

				UserMapper mapperForThisJob = buildUserMapperForThisJob(
						nameParamMap, visitor, currentJob);

				Utilities.getInstance().setMapper(mapperForThisJob);

				// Defining the PDL verificator
				GroupProcessor groupProcessor = new GroupProcessor(service);
				// processing the verifications
				groupProcessor.process();

				ErrorSummary errorsOnThisJob = getSummaryOfErrorPerJob(groupProcessor);

				List<String> errorList = buildErrorsListFromErrorSummary(errorsOnThisJob);
				currentJob.setJobErrors(errorList);

				// If the job is in error
				if (errorsOnThisJob.getHasJobError()) {
					// We notify the users asking for this job that it will be
					// deleted
					MailSenderBusiness.getInstance()
							.notifyPurgeToUserAskedThisJob(currentJob);

					// then we add the current job id to the list of jobs to be
					// deleted from the DB entry
					idJobsToDelete.add(currentId);
				}
			}
			PurgeBusiness.getInstance().deleteJobsFromListIds(idJobsToDelete);

		} catch (Exception e) {
			// Nothing to do
			e.printStackTrace();
		}

	}

	private Service buildPDLModelObjectFromDescription()
			throws MalformedURLException, IOException, SQLException,
			ClassNotFoundException, JAXBException {
		String pdlFileURL = GlobalTechConfigBusiness.getInstance()
		.getServletContainer() + "pdlDescription/PDL-Description.xml";

		System.out.println("file location == " + pdlFileURL);

		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new URL(pdlFileURL).openConnection()
						.getInputStream()));

		StringBuilder sb = new StringBuilder();
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			sb.append(line);
			sb.append("\n");
		}
		bufferedReader.close();
		String xmlMessage = sb.toString();

		ByteArrayInputStream input = new ByteArrayInputStream(
				xmlMessage.getBytes());

		JAXBContext jaxbContext = JAXBContext
				.newInstance("net.ivoa.parameter.model");
		
		Unmarshaller u = jaxbContext.createUnmarshaller();
		
		return  (Service) u.unmarshal(input);
	}

	private List<String> buildErrorsListFromErrorSummary(
			ErrorSummary errorsOnThisJob) {

		if (errorsOnThisJob.getHasJobError()) {

			List<String> toReturn = new ArrayList<String>();

			for (Entry<String, List<StatementHelperContainer>> entry : errorsOnThisJob
					.getErrorsPerGroup().entrySet()) {
				for (int i = 0; i < entry.getValue().size(); i++) {
					String tempString = "Error on group" + entry.getKey()
							+ ": the constraint '+"
							+ entry.getValue().get(i).getStatementComment()
							+ "' is violated";
					toReturn.add(tempString);
				}
			}
			return toReturn;
		} else {
			return null;
		}
	}

	private ErrorSummary getSummaryOfErrorPerJob(GroupProcessor groupProcessor) {
		List<GroupHandlerHelper> handler = groupProcessor.getGroupsHandler();

		ErrorSummary errorOnThisJob = new ErrorSummary();

		Map<String, List<StatementHelperContainer>> errorsPerGroup = new HashMap<String, List<StatementHelperContainer>>();

		// Loop for every group
		for (int i = 0; i < handler.size(); i++) {
			String currentGroupName = handler.get(i).getGroupName();
			Boolean isGroupInError = false;
			List<StatementHelperContainer> statementInErrorInThisGroup = new ArrayList<StatementHelperContainer>();
			// Loop for every statement in the current group
			if (null != handler.get(i).getStatementHelperList()) {
				for (StatementHelperContainer currentStatement : handler.get(i)
						.getStatementHelperList()) {
					if (currentStatement.isStatementSwitched()) {
						if (null != currentStatement.isStatementValid()) {

							isGroupInError = isGroupInError
									|| !currentStatement.isStatementValid();

							if (!currentStatement.isStatementValid()) {
								statementInErrorInThisGroup
										.add(currentStatement);
							}
						}

					}
				}
				// The group is in error if at least one of statement is in
				// error
				if (isGroupInError) {
					errorOnThisJob.setHasJobError(true);
				}
			}
			errorsPerGroup.put(currentGroupName, statementInErrorInThisGroup);
		}
		errorOnThisJob.setErrorsPerGroup(errorsPerGroup);
		return errorOnThisJob;
	}

	private UserMapper buildUserMapperForThisJob(
			Map<String, SingleParameter> nameParamMap,
			GeneralParameterVisitor visitor, JobBean currentJob) {
		// Storing in the following map the couples 'paramName, paramValues'
		// defining the present configurationX
		Map<String, String> valuesForParamsInDB = currentJob
				.getJobConfiguration();

		UserMapper currentMapper = new UserMapper();

		for (Map.Entry<String, String> entry : valuesForParamsInDB.entrySet()) {
			String paramName = entry.getKey();
			String paramValue = entry.getValue();
			String paramType = nameParamMap.get(paramName).getParameterType()
					.toString();
			String paramDescription = nameParamMap.get(paramName)
					.getSkossConcept();
			GeneralParameter currentParam = new GeneralParameter(paramValue,
					paramType, paramDescription, visitor);

			currentMapper.setSingleValueInMap(paramName, currentParam);
		}
		return currentMapper;
	}

}
