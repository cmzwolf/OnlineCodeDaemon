package net.ivoa.oc.daemon.runner;

/**
 * @author Carlo Maria Zwolf
 * Observatoire de Paris
 * LERMA
 */

import java.sql.SQLException;

import org.apache.commons.mail.EmailException;

import net.ivoa.oc.daemon.jobProcessor.JobProcessor;
import net.ivoa.oc.daemon.jobProcessor.Purger;
import net.ivoa.oc.daemon.jobProcessor.ResultsProcessor;
import net.ivoa.oc.daemon.pdlverification.PDLverifier;
import net.ivoa.pdr.business.MailSenderBusiness;

public class Daemon {
	public static void main(String[] args) throws SQLException,
			ClassNotFoundException, EmailException {
		PDLverifier.getInstance().performPDLVerificationOnJobs();
		Purger.getInstance().purge();
		JobProcessor.getInstance().processJobs();
		ResultsProcessor.getInstance().processResults();
		MailSenderBusiness.getInstance().notifyUsersOfAvailableResults();
	}
}
