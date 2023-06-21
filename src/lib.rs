pub mod panda_client;

#[cfg(feature = "reqwest")]
pub mod panda_client_reqwest;

#[cfg(feature = "esp32")]
pub mod panda_client_esp;

#[cfg(feature = "spin")]
pub mod panda_client_spin;
