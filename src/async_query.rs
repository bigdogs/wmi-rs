//! # Async query support
//! This module does not export anything, as it provides additional
//! methods on [`WMIConnection`](WMIConnection)
//!
//! You only have to activate the `async-query` feature flag in Cargo.toml to use them.
//! ```toml
//! wmi = { version = "x.y.z",  features = ["async-query"] }
//! ```
//!
//!

use crate::query_sink::{IWbemObjectSink, QuerySink};
use crate::result_enumerator::IWbemClassWrapper;
use crate::WMIError;
use crate::{connection::WMIConnection, utils::check_hres, WMIResult};
use crate::{
    query::{build_query, FilterValue},
    BStr,
};
use com::production::ClassAllocation;
use com::AbiTransferable;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use serde::de;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::{collections::HashMap, ptr};
use winapi::um::oaidl::VARIANT;
use winapi::um::unknwnbase::IUnknown;
use winapi::um::wbemcli::{
    IID_IWbemClassObject, IWbemClassObject, WBEM_FLAG_BIDIRECTIONAL, WBEM_FLAG_SEND_STATUS,
};

///
/// ### Additional async methods
/// **Following methods are implemented under the
/// `async-query` feature flag.**
///
impl WMIConnection {
    /// Wrapper for the [ExecQueryAsync](https://docs.microsoft.com/en-us/windows/win32/api/wbemcli/nf-wbemcli-iwbemservices-execqueryasync)
    /// method. Provides safety checks, and returns results
    /// as a Stream instead of the original Sink.
    ///
    pub fn exec_query_async_native_wrapper(
        &self,
        query: impl AsRef<str>,
    ) -> WMIResult<impl Stream<Item = WMIResult<IWbemClassWrapper>>> {
        let query_language = BStr::from_str("WQL")?;
        let query = BStr::from_str(query.as_ref())?;

        let (tx, rx) = async_channel::unbounded();
        // The internal RefCount has initial value = 1.
        let p_sink: ClassAllocation<QuerySink> = QuerySink::allocate(Some(tx));
        let p_sink_handel = IWbemObjectSink::from(&**p_sink);

        unsafe {
            // As p_sink's RefCount = 1 before this call,
            // p_sink won't be dropped at the end of ExecQueryAsync
            check_hres((*self.svc()).ExecQueryAsync(
                query_language.as_bstr(),
                query.as_bstr(),
                WBEM_FLAG_BIDIRECTIONAL as i32,
                ptr::null_mut(),
                p_sink_handel.get_abi().as_ptr() as *mut _,
            ))?;
        }

        Ok(rx)
    }

    /// Async version of [`raw_query`](WMIConnection#method.raw_query)
    /// Execute a free-text query and deserialize the results.
    /// Can be used either with a struct (like `query` and `filtered_query`),
    /// but also with a generic map.
    ///
    /// ```edition2018
    /// # use wmi::*;
    /// # use std::collections::HashMap;
    /// # use futures::executor::block_on;
    /// # fn main() -> Result<(), wmi::WMIError> {
    /// #   block_on(exec_async_query())?;
    /// #   Ok(())
    /// # }
    /// #
    /// # async fn exec_async_query() -> WMIResult<()> {
    /// # let con = WMIConnection::new(COMLibrary::new()?.into())?;
    /// use futures::stream::TryStreamExt;
    /// let results: Vec<HashMap<String, Variant>> = con.async_raw_query("SELECT Name FROM Win32_OperatingSystem").await?;
    /// #   Ok(())
    /// # }
    /// ```
    pub async fn async_raw_query<T>(&self, query: impl AsRef<str>) -> WMIResult<Vec<T>>
    where
        T: de::DeserializeOwned,
    {
        self.exec_query_async_native_wrapper(query)?
            .map(|item| match item {
                Ok(wbem_class_obj) => wbem_class_obj.into_desr(),
                Err(e) => Err(e),
            })
            .try_collect::<Vec<_>>()
            .await
    }

    /// Query all the objects of type T.
    ///
    /// ```edition2018
    /// # use wmi::*;
    /// # use std::collections::HashMap;
    /// # use futures::executor::block_on;
    /// # fn main() -> Result<(), wmi::WMIError> {
    /// #   block_on(exec_async_query())?;
    /// #   Ok(())
    /// # }
    /// #
    /// # async fn exec_async_query() -> WMIResult<()> {
    /// # let con = WMIConnection::new(COMLibrary::new()?.into())?;
    /// use serde::Deserialize;
    /// #[derive(Deserialize, Debug)]
    /// struct Win32_Process {
    ///     Name: String,
    /// }
    ///
    /// let procs: Vec<Win32_Process> = con.async_query().await?;
    /// #   Ok(())
    /// # }
    /// ```
    pub async fn async_query<T>(&self) -> WMIResult<Vec<T>>
    where
        T: de::DeserializeOwned,
    {
        let query_text = build_query::<T>(None)?;

        self.async_raw_query(&query_text).await
    }

    /// Query all the objects of type T, while filtering according to `filters`.
    ///
    pub async fn async_filtered_query<T>(
        &self,
        filters: &HashMap<String, FilterValue>,
    ) -> WMIResult<Vec<T>>
    where
        T: de::DeserializeOwned,
    {
        let query_text = build_query::<T>(Some(&filters))?;

        self.async_raw_query(&query_text).await
    }

    pub fn async_notification_query<'a, T>(
        &'a self,
        wql: &str,
    ) -> WMIResult<NotificationReceiver<'a, T>> {
        let query_language = BStr::from_str("WQL")?;
        let query = BStr::from_str(wql)?;

        let (tx, rx) = async_channel::unbounded();
        // The internal RefCount has initial value = 1.
        let p_sink: ClassAllocation<QuerySink> = QuerySink::allocate(Some(tx));
        let p_sink_handel = IWbemObjectSink::from(&**p_sink);

        unsafe {
            // As p_sink's RefCount = 1 before this call,
            // p_sink won't be dropped at the end of ExecQueryAsync
            check_hres((*self.svc()).ExecNotificationQueryAsync(
                query_language.as_bstr(),
                query.as_bstr(),
                WBEM_FLAG_SEND_STATUS as i32,
                ptr::null_mut(),
                p_sink_handel.get_abi().as_ptr() as *mut _,
            ))?;
        }

        Ok(NotificationReceiver::<'a, T> {
            rx,
            sink: p_sink,
            conn: self,
            _data: PhantomData,
        })
    }
}

pub struct NotificationReceiver<'a, T> {
    rx: async_channel::Receiver<WMIResult<IWbemClassWrapper>>,
    sink: ClassAllocation<QuerySink>,
    conn: &'a WMIConnection,
    _data: PhantomData<T>,
}

impl<'a, T> NotificationReceiver<'a, T> {
    pub async fn recv(&self) -> WMIResult<T>
    where
        T: de::DeserializeOwned,
    {
        let r = match self.rx.recv().await {
            Err(e) => {
                log::warn!("channel receive error: {e}");
                return Err(WMIError::RcevError);
            }
            Ok(r) => r?,
        };
        unsafe {
            let target_instance = BStr::from_str("TargetInstance")?;
            let mut vt_prop: VARIANT = unsafe { std::mem::zeroed() };
            let mut cim_type = 0;
            check_hres((*r.inner.as_ptr()).Get(
                target_instance.as_lpcwstr(),
                0,
                &mut vt_prop,
                &mut cim_type,
                std::ptr::null_mut(),
            ))?;
            debug_assert_eq!(cim_type, 13);

            let mut p_wbm: *mut IWbemClassObject = ptr::null_mut();
            unsafe {
                let unknown: *const IUnknown = unsafe { *vt_prop.n1.n2().n3.punkVal() };
                let res = check_hres(
                    (*unknown).QueryInterface(&IID_IWbemClassObject, &mut p_wbm as *mut _ as _),
                );
                (*unknown).Release();
                res?
            }
            // "unwrap" is ok because previous call is success?
            let wrapper = unsafe { IWbemClassWrapper::new(NonNull::new(p_wbm).unwrap()) };
            wrapper.into_desr()
        }
    }

    pub fn cancel(&self) {
        unsafe {
            let p_sink_handel = IWbemObjectSink::from(&**self.sink);
            (*self.conn.svc()).CancelAsyncCall(p_sink_handel.get_abi().as_ptr() as *mut _);
        }
    }
}

impl<'a, T> Drop for NotificationReceiver<'a, T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[cfg(test)]
mod tests {
    use crate::tests::fixtures::*;
    use crate::Variant;
    use futures::stream::StreamExt;
    use std::collections::HashMap;

    #[async_std::test]
    async fn async_it_works_async() {
        let wmi_con = wmi_con();

        let result = wmi_con
            .exec_query_async_native_wrapper("SELECT OSArchitecture FROM Win32_OperatingSystem")
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 1);
    }

    #[async_std::test]
    async fn async_it_handles_invalid_query() {
        let wmi_con = wmi_con();

        let result = wmi_con
            .exec_query_async_native_wrapper("invalid query")
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 0);
    }

    #[async_std::test]
    async fn async_it_provides_raw_query_result() {
        let wmi_con = wmi_con();

        let results: Vec<HashMap<String, Variant>> = wmi_con
            .async_raw_query("SELECT * FROM Win32_GroupUser")
            .await
            .unwrap();

        for res in results {
            match res.get("GroupComponent") {
                Some(Variant::String(s)) => assert!(s != ""),
                _ => assert!(false),
            }

            match res.get("PartComponent") {
                Some(Variant::String(s)) => assert!(s != ""),
                _ => assert!(false),
            }
        }
    }

    #[async_std::test]
    async fn async_notification_query_result() {
        #[allow(dead_code)]
        #[derive(Debug, serde::Deserialize)]
        struct Win32_Process {
            ProcessId: i32,
            ExecutablePath: String,
        }

        let wmi_con = wmi_con();
        let mut c = 0;
        let x = wmi_con.async_notification_query::<Win32_Process>("SELECT *  FROM  __InstanceCreationEvent WITHIN 1 WHERE TargetInstance ISA 'Win32_Process'").unwrap();
        loop {
            let o = x.recv().await;
            c += 1;
            eprintln!("{c}: {o:?}");
            if c == 5 {
                eprintln!("wmi cancel");
                x.cancel();
            }
            if c == 10 {
                break;
            }
        }
    }
}
