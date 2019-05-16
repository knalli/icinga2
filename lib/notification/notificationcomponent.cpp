/* Icinga 2 | (c) 2012 Icinga GmbH | GPLv2+ */

#include "notification/notificationcomponent.hpp"
#include "notification/notificationcomponent-ti.cpp"
#include "base/perfdatavalue.hpp"
#include "base/statsfunction.hpp"


using namespace icinga;

REGISTER_TYPE(NotificationComponent);
REGISTER_STATSFUNCTION(NotificationComponent, &NotificationComponent::StatsFunc);

void NotificationComponent::Start(bool runtimeCreated)
{
	ObjectImpl<NotificationComponent>::Start(runtimeCreated);

	Log(LogInformation, "NotificationComponent")
		<< "'" << GetName() << "' started.";

	Checkable::OnStateChange.connect(std::bind(&NotificationComponent::StateChangeHandler, this, _1, _2, _3));
	Checkable::OnFlappingChanged.connect(std::bind(&NotificationComponent::FlappingChangedHandler, this, _1));

	/* This is never called and does not work currently */
	Notification::OnNextNotificationChanged.connect(std::bind(&NotificationComponent::NextNotificationChangedHandler, this, _1, _2));

	m_Thread = std::thread(std::bind(&NotificationComponent::NotificationThreadProc, this));
}

void NotificationComponent::Stop(bool runtimeRemoved)
{
	m_Thread.join();

	Log(LogInformation, "NotificationComponent")
		<< "'" << GetName() << "' stopped.";

	ObjectImpl<NotificationComponent>::Stop(runtimeRemoved);
}

void NotificationComponent::StatsFunc(const Dictionary::Ptr& status, const Array::Ptr& perfdata)
{
	DictionaryData nodes;

	for (const NotificationComponent::Ptr& notifier : ConfigType::GetObjectsByType<NotificationComponent>()) {
		unsigned long idle = notifier->GetIdleNotifications();
		unsigned long pending = notifier->GetPendingNotifications();

		nodes.emplace_back(notifier->GetName(), new Dictionary({
			{ "idle", idle },
			{ "pending", pending }
		}));

		String perfdata_prefix = "notificationcomponent_" + notifier->GetName() + "_";
		perfdata->Add(new PerfdataValue(perfdata_prefix + "idle", Convert::ToDouble(idle)));
		perfdata->Add(new PerfdataValue(perfdata_prefix + "pending", Convert::ToDouble(pending)));
	}

	status->Set("notificationcomponent", new Dictionary(std::move(nodes)));
}

void NotificationComponent::NextNotificationChangedHandler(const Notification::Ptr& notification, const MessageOrigin::Ptr& origin) {
	Log(LogCritical, "DEBUG")
		<< "GOT IT " << notification->GetName();
	boost::mutex::scoped_lock lock(m_Mutex);

	/* remove and re-insert the object from the set in order to force an index update */
	typedef boost::multi_index::nth_index<NotificationSet, 0>::type MessageView;
	MessageView& idx = boost::get<0>(m_IdleNotifications);

	auto it = idx.find(notification);

	if (it == idx.end())
		return;

	idx.erase(notification);

	NotificationScheduleInfo nsi = GetNotificationScheduleInfo(notification);
	idx.insert(nsi);

	m_CV.notify_all();
}

void NotificationComponent::StateChangeHandler(const Checkable::Ptr& checkable, const CheckResult::Ptr& cr, StateType type) {
	// Need to know if this was a recovery (state = ok?)
	if (!HardStateNotificationCheck(checkable)) {
		Log(LogCritical, "DEBUG") << "Not sending for " << checkable->GetName();
		return;
	}

	Host::Ptr host;
	Service::Ptr service;
	tie(host, service) = GetHostService(checkable);

	if (type != StateTypeHard) {
		Log(LogCritical, "DEBUG")
				<< "Ignoring soft state change for " << checkable->GetName();
		return;
	}

	NotificationType ntype = (cr->GetState() == 0 ? NotificationRecovery : NotificationProblem);

	for (const Notification::Ptr notification : checkable->GetNotifications()) {
		Log(LogCritical, "DEBUG")
			<< "Checkable " << checkable->GetName() << " had a hard change and wants to check Notification "
			<< notification->GetName();

		// Check Filters here
		notification->BeginExecuteNotification(ntype, checkable->GetLastCheckResult(), false, false);

		// Queue Renotifications
		if (ntype != NotificationRecovery) {
			m_IdleNotifications.insert(GetNotificationScheduleInfo(notification));
			m_CV.notify_all();
		}
	}

}

void NotificationComponent::FlappingChangedHandler(const Checkable::Ptr& checkable)
{
	NotificationType ntype = checkable->IsFlapping() ? NotificationFlappingStart : NotificationFlappingEnd;
	Log(LogCritical, "DEBUG") << checkable->GetName() << " is flapping!";
	for (const Notification::Ptr notification : checkable->GetNotifications()) {
		Log(LogCritical, "DEBUG")
				<< "Checkable " << checkable->GetName() << " his flapping and wants to check Notification "
				<< notification->GetName();

		// Check Filters here
		notification->BeginExecuteNotification(ntype, checkable->GetLastCheckResult(), false, false);

		// Queue Renotifications
		if (ntype != NotificationFlappingEnd) {
			m_IdleNotifications.insert(GetNotificationScheduleInfo(notification));
			m_CV.notify_all();
		}
	}
}


void NotificationComponent::NotificationThreadProc()
{
	Utility::SetThreadName("Notification Scheduler");

	boost::mutex::scoped_lock lock(m_Mutex);

	for (;;) {
		typedef boost::multi_index::nth_index<NotificationSet, 1>::type NotificationTimeView;
		NotificationTimeView& idx = boost::get<1>(m_IdleNotifications);
		while (idx.begin() == idx.end() && !m_Stopped)
			m_CV.wait(lock);

		if (m_Stopped)
			break;

		auto it = idx.begin();
		NotificationScheduleInfo nsi = *it;

		double wait = nsi.NextMessage - Utility::GetTime();

		if (wait > 0) {
			m_CV.timed_wait(lock, boost::posix_time::milliseconds(long(wait * 1000)));

			continue;
		}

		Notification::Ptr notification = nsi.Object;
		m_IdleNotifications.erase(notification);

		// Check for execution needed

		nsi = GetNotificationScheduleInfo(notification);

		Log(LogCritical, "NotificationComponent")
				<< "Scheduling info for notification '" << notification->GetName() << "' ("
				<< Utility::FormatDateTime("%Y-%m-%d %H:%M:%S %z", notification->GetNextNotification()) << "): Object '"
				<< nsi.Object->GetName() << "', Next Message: "
				<< Utility::FormatDateTime("%Y-%m-%d %H:%M:%S %z", nsi.NextMessage) << "(" << nsi.NextMessage << ").";

		m_PendingNotifications.insert(nsi);

		lock.unlock();
		Log(LogCritical, "DEBUG", "Please execute");
		Utility::QueueAsyncCallback(std::bind(&NotificationComponent::SendMessageHelper, NotificationComponent::Ptr(this), notification, NotificationProblem, true));
		Log(LogCritical, "DEBUG")
			<< "Executed??? Next one at " << Utility::FormatDateTime("%Y-%m-%d %H:%M:%S %z", notification->GetNextNotification());
		lock.lock();
	}
}

bool NotificationComponent::HardStateNotificationCheck(const Checkable::Ptr& checkable)
{
	bool send_notification = false;

	// Don't send in these cases
	if (!checkable->IsReachable(DependencyNotification) || checkable->IsInDowntime()
	|| checkable->IsAcknowledged() || checkable->IsFlapping()) {
		Log(LogCritical, "DEBUG")
			<< "Not Sending because not reachable | in downtime | acknowledged | flapping: " << checkable->GetName();
		return false;
	}

	// We know checkable is in a Hard State, Second case is Recovery
	if ((checkable->GetLastStateType() == StateTypeSoft) ||
						(checkable->GetLastStateType() == StateTypeHard
						&& checkable->GetLastStateRaw() != ServiceOK
						&& checkable->GetStateRaw() == ServiceOK)) {
		send_notification = true;
		Log(LogCritical, "DEBUG")
			<< "Sending because soft -> hard | recovery: " << checkable->GetName();
	}

	/* Or if the checkable is volatile and in a HARD state. */
	if (checkable->GetVolatile()) {
		send_notification = true;
		Log(LogCritical, "DEBUG")
			<< "Sending because volatile & hard state: " << checkable->GetName();
	}

	if (checkable->GetLastStateRaw() == ServiceOK && checkable->GetLastStateType() == StateTypeSoft) {
		send_notification = false; /* Don't send notifications for SOFT-OK -> HARD-OK. */
		Log(LogCritical, "DEBUG")
			<< "Not sending becuase soft-ok -> hard-ok: " << checkable->GetName();
	}

	if (checkable->GetVolatile() && checkable->GetLastStateRaw() == ServiceOK && checkable->GetStateRaw() == ServiceOK) {
		send_notification = false; /* Don't send notifications for volatile OK -> OK changes. */
		Log(LogCritical, "DEBUG")
			<< "Not sending because volatile & ok -> ok: " << checkable->GetName();
	}

	return send_notification;
}

void NotificationComponent::SendMessageHelper(const Notification::Ptr& notification, NotificationType type, bool reminder) {
	// Check if we need to send here??

	notification->BeginExecuteNotification(type, notification->GetCheckable()->GetLastCheckResult(), false, reminder);

	boost::mutex::scoped_lock lock(m_Mutex);
	auto it = m_PendingNotifications.find(notification);

	if (it != m_PendingNotifications.end()) {
		m_PendingNotifications.erase(it);

		if (notification->IsActive())
			m_IdleNotifications.insert(GetNotificationScheduleInfo(notification));

		m_CV.notify_all();
	}
}

NotificationScheduleInfo NotificationComponent::GetNotificationScheduleInfo(const Notification::Ptr& notification)
{
	NotificationScheduleInfo nsi;
	nsi.Object = notification;
	nsi.NextMessage = notification->GetNextNotification();
	return nsi;
}

unsigned long NotificationComponent::GetIdleNotifications()
{
	boost::mutex::scoped_lock lock(m_Mutex);

	return m_IdleNotifications.size();
}

unsigned long NotificationComponent::GetPendingNotifications()
{
	boost::mutex::scoped_lock lock(m_Mutex);

	return m_PendingNotifications.size();
}
