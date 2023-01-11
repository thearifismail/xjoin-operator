package v1alpha1

func (instance *XJoinSynchronizer) GetUIDString() string {
	return string(instance.GetUID())
}
