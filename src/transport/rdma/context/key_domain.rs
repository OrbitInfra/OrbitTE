//! PD-owned key history and never-bound exhausted MW indices.

use super::*;

pub(super) struct ExhaustedWindow {
    handle: NonNull<std::ffi::c_void>,
    index: u32,
}

// The context mutex serializes these never-bound MWs; their actual PD outlives them.
unsafe impl Send for ExhaustedWindow {}

impl Resources {
    pub(super) fn burn_window_key(
        &mut self,
        memory: &mut sys::MemoryHandles,
        config: &RdmaConfig,
        verbs: &sys::Verbs,
    ) -> Result<u32> {
        loop {
            if self
                .exhausted_windows
                .iter()
                .any(|window| window.index == memory.window_key & !255)
            {
                self.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .window_key_exhaustions += 1;
                return Err(Error::ResourceExhausted);
            }
            let previous = self.keys.len();
            let key = self.keys.burn(memory.window_key);
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.window_key_indices += self.keys.len() - previous;
            match key {
                Ok(key) => {
                    usage.burned_window_keys += 1;
                    return Ok(key);
                }
                Err(error) => {
                    let index = memory.window_key & !255;
                    if !self.keys.exhausted(memory.window_key)
                        || self.exhausted_windows.len() >= config.max_window_key_indices
                    {
                        usage.window_key_exhaustions += 1;
                        return Err(error);
                    }
                    if self.exhausted_windows.try_reserve(1).is_err() {
                        usage.window_key_exhaustions += 1;
                        return Err(Error::ResourceExhausted);
                    }
                    let handle =
                        NonNull::new(memory.mw).ok_or(Error::Internal("missing exhausted MW"))?;
                    self.exhausted_windows
                        .push(ExhaustedWindow { handle, index });
                    memory.mw = std::ptr::null_mut();
                    usage.exhausted_windows += 1;
                    usage.window_key_rollovers += 1;
                    drop(usage);
                    // SAFETY: this slot is empty; the same live PD owns the new MW.
                    let code = unsafe { (verbs.allocate_window)(memory, &self.handles) };
                    if !memory.mw.is_null() {
                        let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
                        usage.active_windows += 1;
                        usage.opened_windows += 1;
                    }
                    check("ibv_alloc_mw rollover", code)?;
                    if memory.mw.is_null() {
                        return Err(Error::Internal("missing replacement MW"));
                    }
                }
            }
        }
    }

    pub(super) fn close_exhausted_windows(&mut self, verbs: &sys::Verbs) -> Result<()> {
        while let Some(window) = self.exhausted_windows.last() {
            let mut memory = sys::MemoryHandles {
                mw: window.handle.as_ptr(),
                ..Default::default()
            };
            // SAFETY: this MW was never bound; only its retained PD is required.
            let code = unsafe { (verbs.dealloc_window)(&mut memory) };
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            if code != 0 {
                usage.retained_windows += self.exhausted_windows.len();
                return Err(Error::Unquiesced {
                    operation: "ibv_dealloc_mw exhausted index",
                    code,
                });
            }
            self.exhausted_windows.pop();
            usage.active_windows -= 1;
            usage.exhausted_windows -= 1;
        }
        Ok(())
    }
}
