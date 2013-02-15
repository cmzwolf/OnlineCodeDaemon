package net.ivoa.oc.daemon.jobProcessor;

/**
 * @author Carlo Maria Zwolf
 * Observatoire de Paris
 * LERMA
 */

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import net.ivoa.pdr.business.JobBusiness;
import net.ivoa.pdr.business.MailSenderBusiness;
import net.ivoa.pdr.business.OutputsBusiness;
import net.ivoa.pdr.business.PurgeBusiness;
import net.ivoa.pdr.commons.IOFile;
import net.ivoa.pdr.commons.JobBean;

import org.apache.commons.mail.EmailException;

public class Purger {
	private static final Purger instance = new Purger();

	public static Purger getInstance() {
		return instance;
	}

	private Purger() {
	}

	public void purge() throws SQLException, ClassNotFoundException, EmailException {
		List<Integer> jobsToDelete = PurgeBusiness.getInstance()
				.getIdJobsToOld();
		List<IOFile> outputs = OutputsBusiness.getInstance()
				.getPatternInputFile();

		// for every job to delete
		for (Integer currentJobId : jobsToDelete) {
			
			JobBean job = JobBusiness.getInstance().getJobBeanFromIdJob(currentJobId);
			//We notify the users that asked this job
			MailSenderBusiness.getInstance().notifyPurgeToUserAskedThisJob(job);
			
			// we delete all its outputs
			for (IOFile currentFile : outputs) {
				String filename = currentFile.getFileDirectory() + "/"
						+ currentJobId + "." + currentFile.getFileExtension();
				File file = new File(filename);
				file.delete();
			}
		}

		// And we remove the entry of this jobs from the DB
		PurgeBusiness.getInstance().deleteJobsFromListIds(jobsToDelete);
	}

	
}
